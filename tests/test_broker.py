"""Integration coverage for the Redis Streams broker."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncGenerator
from contextlib import suppress
from typing import Any

import pytest
from redis.asyncio import Redis
from redis.exceptions import RedisError
from taskiq import AckableMessage
from taskiq.message import BrokerMessage
from taskiq.utils import maybe_awaitable

from taskiq_redis_streams import RedisStreamsBroker
from taskiq_redis_streams import broker as broker_module
from taskiq_redis_streams.broker import ABANDONED_CONSUMER


def make_broker(redis_url: str, **kwargs: Any) -> RedisStreamsBroker:
    """Build a broker whose Redis keys cannot overlap another test's keys."""
    xread_block = kwargs.pop("xread_block", 20)
    return RedisStreamsBroker(
        redis_url,
        queue_name="jobs",
        namespace=f"test-{uuid.uuid4().hex}",
        xread_block=xread_block,
        **kwargs,
    )


def raw_message(data: bytes = b"payload") -> BrokerMessage:
    """Create an opaque broker payload."""
    return BrokerMessage(
        task_id=uuid.uuid4().hex,
        task_name="test.task",
        message=data,
        labels={},
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


def test_broker_generates_unique_consumer_names() -> None:
    """Each broker instance owns a fresh Redis consumer identity."""
    first = RedisStreamsBroker("redis://127.0.0.1:7000/14")
    second = RedisStreamsBroker("redis://127.0.0.1:7000/14")

    assert first.consumer_name.startswith("worker-")
    assert first.consumer_name != second.consumer_name


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
    assert broker.consumer_group_name.endswith(":workers:jobs")
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
    finally:
        await redis.aclose()
        await listener.aclose()
        await broker.shutdown()


@pytest.mark.asyncio
async def test_reclaims_entry_whose_consumer_heartbeat_has_expired(
    redis_url: str,
) -> None:
    """An entry held by a consumer without a lease is recovered."""
    broker = make_broker(
        redis_url,
        consumer_heartbeat_interval=10,
        consumer_heartbeat_ttl=20,
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
        await asyncio.sleep(0.02)

        listener = broker.listen()
        received = await next_message(listener)
        assert received.data == b"payload"
        await acknowledge(received)
        await listener.aclose()
    finally:
        await redis.aclose()
        await broker.shutdown()


@pytest.mark.asyncio
async def test_live_consumer_heartbeat_prevents_reclaim(redis_url: str) -> None:
    """A healthy worker retains a long-running entry regardless of its idle age."""
    first = make_broker(
        redis_url,
        consumer_heartbeat_interval=10,
        consumer_heartbeat_ttl=30,
        reclaim_interval=0,
    )
    await first.startup()
    await first.kick(raw_message())
    first_listener = first.listen()
    received = await next_message(first_listener)

    replacement = RedisStreamsBroker(
        redis_url,
        queue_name=first.queue_name,
        namespace=first.namespace,
        xread_block=20,
        consumer_heartbeat_interval=10,
        consumer_heartbeat_ttl=30,
        reclaim_interval=0,
    )
    await replacement.startup()
    replacement_listener = replacement.listen()
    redis = Redis.from_url(redis_url)
    try:
        await asyncio.sleep(0.05)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(anext(replacement_listener), timeout=0.1)

        pending = await redis.xpending_range(
            first.stream_name,
            first.consumer_group_name,
            min="-",
            max="+",
            count=10,
        )
        assert as_text(pending[0]["consumer"]) == first.consumer_name
        assert await redis.exists(first.consumer_heartbeat_key)
        await acknowledge(received)
    finally:
        await redis.aclose()
        await replacement_listener.aclose()
        await replacement.shutdown()
        await first_listener.aclose()
        await first.shutdown()


@pytest.mark.asyncio
async def test_heartbeat_expires_after_configured_lease_ttl(
    redis_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed heartbeat refresh eventually lets the Redis lease expire."""
    broker = make_broker(
        redis_url,
        consumer_heartbeat_interval=10,
        consumer_heartbeat_ttl=30,
    )
    await broker.startup()
    await broker.kick(raw_message())
    listener = broker.listen()
    received = await next_message(listener)
    redis = Redis.from_url(redis_url)

    async def unavailable_heartbeat() -> None:
        raise RedisError("temporary connection failure")

    monkeypatch.setattr(broker, "_refresh_heartbeat", unavailable_heartbeat)
    try:
        assert broker.consumer_heartbeat_ttl == 30
        await asyncio.sleep(0.1)
        assert not await redis.exists(broker.consumer_heartbeat_key)
        await acknowledge(received)
    finally:
        await redis.aclose()
        await listener.aclose()
        await broker.shutdown()


@pytest.mark.asyncio
async def test_heartbeat_uses_a_dedicated_connection_pool(redis_url: str) -> None:
    """A blocking listener cannot prevent heartbeat lease renewal."""
    broker = make_broker(
        redis_url,
        xread_block=0,
        max_connection_pool_size=1,
        consumer_heartbeat_interval=10,
        consumer_heartbeat_ttl=60,
    )
    await broker.startup()
    await broker.kick(raw_message())
    listener = broker.listen()
    received = await next_message(listener)
    blocking_read = asyncio.create_task(anext(listener))
    redis = Redis.from_url(redis_url)
    try:
        await asyncio.sleep(0.1)
        assert await redis.exists(broker.consumer_heartbeat_key)
    finally:
        blocking_read.cancel()
        with suppress(asyncio.CancelledError):
            await blocking_read
        await listener.aclose()
        await acknowledge(received)
        await redis.aclose()
        await broker.shutdown()


@pytest.mark.asyncio
async def test_clean_shutdown_allows_immediate_reclaim(redis_url: str) -> None:
    """A removed heartbeat does not make a replacement wait for the lease TTL."""
    first = make_broker(
        redis_url,
        consumer_heartbeat_interval=10,
        consumer_heartbeat_ttl=10_000,
        reclaim_interval=0,
    )
    await first.startup()
    await first.kick(raw_message())
    first_listener = first.listen()
    await next_message(first_listener)
    await first_listener.aclose()
    await first.shutdown()

    replacement = RedisStreamsBroker(
        redis_url,
        queue_name=first.queue_name,
        namespace=first.namespace,
        xread_block=20,
        consumer_heartbeat_interval=10,
        consumer_heartbeat_ttl=10_000,
        reclaim_interval=0,
    )
    await replacement.startup()
    replacement_listener = replacement.listen()
    try:
        recovered = await next_message(replacement_listener)
        assert recovered.data == b"payload"
        await acknowledge(recovered)
    finally:
        await replacement_listener.aclose()
        await replacement.shutdown()


@pytest.mark.asyncio
async def test_listener_close_keeps_heartbeat_until_broker_shutdown(
    redis_url: str,
) -> None:
    """Taskiq can drain running tasks after listener cancellation."""
    broker = make_broker(redis_url)
    await broker.startup()
    await broker.kick(raw_message())
    listener = broker.listen()
    received = await next_message(listener)
    redis = Redis.from_url(redis_url)
    try:
        await listener.aclose()
        assert await redis.exists(broker.consumer_heartbeat_key)
        await acknowledge(received)
    finally:
        await broker.shutdown()
        assert not await redis.exists(broker.consumer_heartbeat_key)
        await redis.aclose()


@pytest.mark.asyncio
async def test_listen_retries_after_redis_error(
    redis_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient Redis error does not terminate the listener."""
    broker = make_broker(redis_url)
    await broker.startup()
    await broker.kick(raw_message())
    original_read = broker._read_new_entries
    attempts = 0

    async def flaky_read(redis: Redis, count: int | None) -> list[Any]:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RedisError("temporary connection failure")
        return await original_read(redis, count)

    monkeypatch.setattr(broker_module, "RETRY_INITIAL_DELAY", 0)
    monkeypatch.setattr(broker, "_read_new_entries", flaky_read)
    listener = broker.listen()
    try:
        received = await next_message(listener)
        assert received.data == b"payload"
        assert attempts == 2
        await acknowledge(received)
    finally:
        await listener.aclose()
        await broker.shutdown()


@pytest.mark.asyncio
async def test_listen_retries_a_failed_reclaim_scan_immediately(
    redis_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed reclaim scan does not consume the reclaim interval."""
    broker = make_broker(redis_url)
    await broker.startup()
    await broker.kick(raw_message())
    original_claim = broker._claim_orphaned_entries
    attempts = 0

    async def flaky_claim(
        redis: Redis,
        limit: int,
        protected: set[str],
        pending_start: str,
    ) -> tuple[list[Any], str]:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RedisError("temporary connection failure")
        return await original_claim(redis, limit, protected, pending_start)

    monkeypatch.setattr(broker_module, "RETRY_INITIAL_DELAY", 0)
    monkeypatch.setattr(broker, "_claim_orphaned_entries", flaky_claim)
    listener = broker.listen()
    try:
        received = await next_message(listener)
        assert received.data == b"payload"
        assert attempts == 2
        await acknowledge(received)
    finally:
        await listener.aclose()
        await broker.shutdown()


@pytest.mark.asyncio
async def test_listen_cancellation_is_not_retried(
    redis_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Taskiq shutdown cancellation exits a backoff sleep immediately."""
    broker = make_broker(redis_url)
    await broker.startup()
    attempted_read = asyncio.Event()

    async def unavailable_read(redis: Redis, count: int | None) -> list[Any]:
        attempted_read.set()
        raise RedisError("temporary connection failure")

    monkeypatch.setattr(broker_module, "RETRY_INITIAL_DELAY", 60)
    monkeypatch.setattr(broker, "_read_new_entries", unavailable_read)
    listener = broker.listen()
    listener_task = asyncio.create_task(anext(listener))
    try:
        await attempted_read.wait()
        await asyncio.sleep(0)
        listener_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await listener_task
    finally:
        await listener.aclose()
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
