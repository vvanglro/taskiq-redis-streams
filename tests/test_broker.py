"""Integration coverage for the Redis Streams broker."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncGenerator
from typing import Any

import pytest
from redis.asyncio import Redis
from taskiq import AckableMessage
from taskiq.message import BrokerMessage, TaskiqMessage
from taskiq.utils import maybe_awaitable

from taskiq_redis_streams import RedisStreamsBroker
from taskiq_redis_streams.broker import ABANDONED_CONSUMER


def make_broker(redis_url: str, **kwargs: Any) -> RedisStreamsBroker:
    """Build a broker whose Redis keys cannot overlap another test's keys."""
    return RedisStreamsBroker(
        redis_url,
        queue_name="jobs",
        namespace=f"test-{uuid.uuid4().hex}",
        xread_block=20,
        **kwargs,
    )


def raw_message(data: bytes = b"payload") -> BrokerMessage:
    """Create an opaque broker payload without a Taskiq timeout label."""
    return BrokerMessage(
        task_id=uuid.uuid4().hex,
        task_name="test.task",
        message=data,
        labels={},
    )


def timeout_message(broker: RedisStreamsBroker, timeout: float) -> BrokerMessage:
    """Create a serialized Taskiq message with the task timeout label."""
    return broker.formatter.dumps(
        TaskiqMessage(
            task_id=uuid.uuid4().hex,
            task_name="test.task",
            labels={"timeout": timeout},
            args=[],
            kwargs={},
        ),
    )


async def next_message(
    listener: AsyncGenerator[AckableMessage, None],
) -> AckableMessage:
    """Get one listener entry without letting a hung test wait indefinitely."""
    return await asyncio.wait_for(anext(listener), timeout=2)


async def acknowledge(message: AckableMessage) -> None:
    """Run an AckableMessage callback regardless of its sync/async annotation."""
    await maybe_awaitable(message.ack())


def as_text(value: bytes | str | int) -> str:
    """Normalize a Redis response field used in assertions."""
    return value.decode() if isinstance(value, bytes) else str(value)


@pytest.mark.asyncio
async def test_namespaced_stream_round_trip_and_ack(redis_url: str) -> None:
    """The broker stores and acknowledges an entry in its namespaced Stream."""
    broker = make_broker(redis_url)
    await broker.startup()
    await broker.kick(raw_message())

    listener = broker.listen()
    received = await next_message(listener)

    assert received.data == b"payload"
    assert broker.stream_name.endswith(":stream:jobs")
    await acknowledge(received)

    redis = Redis.from_url(redis_url)
    try:
        pending = await redis.xpending_range(
            broker.stream_name,
            broker.consumer_group_name,
            min="-",
            max="+",
            count=10,
        )
        assert pending == []
        assert len(await redis.xrange(broker.stream_name) or []) == 1
    finally:
        await redis.aclose()
        await listener.aclose()
        await broker.shutdown()


@pytest.mark.asyncio
async def test_delete_after_ack_removes_stream_entry(redis_url: str) -> None:
    """The optional cleanup deletes an entry only after it is acknowledged."""
    broker = make_broker(redis_url, delete_after_ack=True)
    await broker.startup()
    await broker.kick(raw_message())

    listener = broker.listen()
    received = await next_message(listener)
    await acknowledge(received)

    redis = Redis.from_url(redis_url)
    try:
        assert await redis.xrange(broker.stream_name) == []
        pending = await redis.xpending_range(
            broker.stream_name,
            broker.consumer_group_name,
            min="-",
            max="+",
            count=10,
        )
        assert pending == []
    finally:
        await redis.aclose()
        await listener.aclose()
        await broker.shutdown()


@pytest.mark.asyncio
async def test_task_timeout_controls_reclaim_deadline(redis_url: str) -> None:
    """A task-specific timeout overrides the broker fallback reclaim deadline."""
    broker = make_broker(
        redis_url,
        reclaim_timeout=10_000,
        reclaim_timeout_grace=0,
        reclaim_interval=0,
    )
    await broker.startup()
    message = timeout_message(broker, timeout=0.02)
    await broker.kick(message)

    redis = Redis.from_url(redis_url)
    try:
        await redis.xreadgroup(
            broker.consumer_group_name,
            "crashed-worker",
            {broker.stream_name: ">"},
            count=1,
        )
        await asyncio.sleep(0.05)

        listener = broker.listen()
        received = await next_message(listener)
        assert received.data == message.message
        await acknowledge(received)
        await listener.aclose()
    finally:
        await redis.aclose()
        await broker.shutdown()


@pytest.mark.asyncio
async def test_fallback_reclaim_deadline_for_non_taskiq_payload(redis_url: str) -> None:
    """Opaque payloads use the configured fallback reclaim deadline."""
    broker = make_broker(
        redis_url,
        reclaim_timeout=20,
        reclaim_interval=0,
    )
    await broker.startup()
    await broker.kick(raw_message())

    redis = Redis.from_url(redis_url)
    try:
        await redis.xreadgroup(
            broker.consumer_group_name,
            "crashed-worker",
            {broker.stream_name: ">"},
            count=1,
        )
        await asyncio.sleep(0.05)

        listener = broker.listen()
        received = await next_message(listener)
        assert received.data == b"payload"
        await acknowledge(received)
        await listener.aclose()
    finally:
        await redis.aclose()
        await broker.shutdown()


@pytest.mark.asyncio
async def test_reclaim_can_be_disabled(redis_url: str) -> None:
    """Disabled reclaim leaves a crashed consumer's pending entry untouched."""
    broker = make_broker(
        redis_url,
        reclaim_enabled=False,
        reclaim_timeout=1,
        reclaim_interval=0,
    )
    await broker.startup()
    await broker.kick(raw_message())

    redis = Redis.from_url(redis_url)
    listener = broker.listen()
    try:
        await redis.xreadgroup(
            broker.consumer_group_name,
            "crashed-worker",
            {broker.stream_name: ">"},
            count=1,
        )
        await asyncio.sleep(0.05)

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(anext(listener), timeout=0.1)

        pending = await redis.xpending_range(
            broker.stream_name,
            broker.consumer_group_name,
            min="-",
            max="+",
            count=10,
        )
        assert len(pending) == 1
        assert as_text(pending[0]["consumer"]) == "crashed-worker"
    finally:
        await listener.aclose()
        await redis.aclose()
        await broker.shutdown()


@pytest.mark.asyncio
async def test_max_pending_limits_entries_reserved_in_pel(redis_url: str) -> None:
    """Local backpressure keeps a second entry out of the PEL until ACK."""
    broker = make_broker(redis_url, xread_count=10, max_pending=1)
    await broker.startup()
    await broker.kick(raw_message(b"first"))
    await broker.kick(raw_message(b"second"))

    listener = broker.listen()
    first = await next_message(listener)
    redis = Redis.from_url(redis_url)
    try:
        pending = await redis.xpending_range(
            broker.stream_name,
            broker.consumer_group_name,
            min="-",
            max="+",
            count=10,
        )
        assert len(pending) == 1

        await acknowledge(first)
        second = await next_message(listener)
        assert {first.data, second.data} == {b"first", b"second"}
        await acknowledge(second)
    finally:
        await redis.aclose()
        await listener.aclose()
        await broker.shutdown()


@pytest.mark.asyncio
async def test_close_hands_unyielded_buffer_to_abandoned_consumer(
    redis_url: str,
) -> None:
    """Closing a listener makes its locally buffered entries recoverable now."""
    broker = make_broker(
        redis_url,
        xread_count=2,
        max_pending=2,
        reclaim_timeout=60_000,
        reclaim_interval=0,
    )
    await broker.startup()
    await broker.kick(raw_message(b"first"))
    await broker.kick(raw_message(b"second"))

    listener = broker.listen()
    first = await next_message(listener)
    await listener.aclose()

    redis = Redis.from_url(redis_url)
    try:
        pending = await redis.xpending_range(
            broker.stream_name,
            broker.consumer_group_name,
            min="-",
            max="+",
            count=10,
        )
        assert any(as_text(item["consumer"]) == ABANDONED_CONSUMER for item in pending)

        replacement = RedisStreamsBroker(
            redis_url,
            queue_name=broker.queue_name,
            namespace=broker.namespace,
            xread_block=20,
            reclaim_timeout=60_000,
            reclaim_interval=0,
        )
        await replacement.startup()
        replacement_listener = replacement.listen()
        recovered = await next_message(replacement_listener)
        assert recovered.data != first.data
        await acknowledge(recovered)
        await replacement_listener.aclose()
        await replacement.shutdown()
    finally:
        await redis.aclose()
        await broker.shutdown()
