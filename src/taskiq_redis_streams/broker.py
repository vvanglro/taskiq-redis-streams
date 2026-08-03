"""Redis Streams broker with bounded local prefetch and heartbeat recovery."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import suppress
from logging import getLogger
from typing import Any, TypeAlias, cast

from redis.asyncio import BlockingConnectionPool, Redis
from redis.exceptions import RedisError, ResponseError
from taskiq import AckableMessage
from taskiq.abc.broker import AsyncBroker
from taskiq.message import BrokerMessage

from taskiq_redis_streams.keys import (
    consumer_group_key,
    consumer_heartbeat_key,
    stream_key,
)

logger = getLogger(__name__)

ABANDONED_CONSUMER = "abandoned"
ABANDONED_IDLE_MS = 10**12
RETRY_INITIAL_DELAY = 0.1
RETRY_MAX_DELAY = 5.0

_CLAIM_IF_HEARTBEAT_MISSING = """
if redis.call('EXISTS', KEYS[1]) == 1 then
    return false
end
local claimed = redis.call(
    'XCLAIM', KEYS[2], ARGV[1], ARGV[2], ARGV[3], ARGV[4], 'JUSTID'
)
if #claimed == 0 then
    return false
end
return claimed[1]
"""

StreamEntry: TypeAlias = tuple[str, dict[bytes, bytes]]
ConnectionPool: TypeAlias = BlockingConnectionPool


class RedisStreamsBroker(AsyncBroker):
    """A single-queue Taskiq broker with heartbeat-based Redis Stream recovery.

    Redis Streams provide at-least-once delivery. Task handlers must therefore
    be idempotent: an unacknowledged entry may be reclaimed by another worker.
    """

    def __init__(
        self,
        url: str,
        *,
        queue_name: str = "taskiq",
        namespace: str = "taskiq",
        xread_block: int = 2_000,
        xread_count: int = 100,
        max_pending: int | None = 100,
        maxlen: int | None = None,
        approximate: bool = True,
        reclaim_interval: int = 30_000,
        reclaim_batch_size: int = 100,
        consumer_heartbeat_interval: int = 10_000,
        consumer_heartbeat_ttl: int = 30_000,
        max_connection_pool_size: int | None = None,
        **connection_kwargs: Any,
    ) -> None:
        """Create a Redis Streams broker.

        :param url: Redis connection URL.
        :param queue_name: Taskiq queue represented by this broker.
        :param namespace: Prefix used for the stream and default group keys.
        :param xread_block: Maximum XREADGROUP block time in milliseconds.
        :param xread_count: Maximum number of entries fetched per XREADGROUP.
        :param max_pending: Maximum entries delivered to this listener but not
            successfully acknowledged. ``None`` disables the local cap.
        :param maxlen: Optional Redis Stream length limit.
        :param approximate: Use Redis's approximate stream trimming when set.
        :param reclaim_interval: Milliseconds between PEL recovery scans. Set
            to ``0`` to scan before every read.
        :param reclaim_batch_size: Maximum PEL entries examined in one scan.
        :param consumer_heartbeat_interval: Milliseconds between liveness
            lease renewals for this Redis consumer.
        :param consumer_heartbeat_ttl: Milliseconds an unrefreshed consumer
            heartbeat remains live before its pending entries are reclaimable.
        :param max_connection_pool_size: Maximum Redis connections in the pool.
        :param connection_kwargs: Extra arguments accepted by redis-py.
        """
        super().__init__()

        if not namespace:
            raise ValueError("namespace must not be empty")
        if xread_block < 0:
            raise ValueError("xread_block must be non-negative")
        if xread_count <= 0:
            raise ValueError("xread_count must be greater than zero")
        if max_pending is not None and max_pending <= 0:
            raise ValueError("max_pending must be greater than zero or None")
        if reclaim_interval < 0:
            raise ValueError("reclaim_interval must be non-negative")
        if reclaim_batch_size <= 0:
            raise ValueError("reclaim_batch_size must be greater than zero")
        if consumer_heartbeat_interval <= 0:
            raise ValueError("consumer_heartbeat_interval must be greater than zero")
        if consumer_heartbeat_ttl <= consumer_heartbeat_interval:
            raise ValueError(
                "consumer_heartbeat_ttl must be greater than "
                "consumer_heartbeat_interval",
            )

        # Stream payloads must remain bytes because Taskiq formatters operate on
        # bytes. Do not let a caller accidentally enable decode_responses.
        connection_kwargs["decode_responses"] = False
        self.connection_pool: ConnectionPool = BlockingConnectionPool.from_url(
            url,
            max_connections=max_connection_pool_size,
            **connection_kwargs,
        )
        self.queue_name = queue_name
        self.namespace = namespace
        self.stream_name = stream_key(queue_name, namespace)
        self.consumer_group_name = consumer_group_key(
            queue_name,
            namespace,
        )
        self.consumer_name = f"worker-{uuid.uuid4().hex}"
        self.consumer_heartbeat_key = consumer_heartbeat_key(
            queue_name,
            namespace,
            self.consumer_name,
        )
        self.xread_block = xread_block
        self.xread_count = xread_count
        self.max_pending = max_pending
        self.maxlen = maxlen
        self.approximate = approximate
        self.reclaim_interval = reclaim_interval
        self.reclaim_batch_size = reclaim_batch_size
        self.consumer_heartbeat_interval = consumer_heartbeat_interval
        self.consumer_heartbeat_ttl = consumer_heartbeat_ttl
        self._heartbeat_task: asyncio.Task[None] | None = None

    async def startup(self) -> None:
        """Create the Redis consumer group before receiving tasks."""
        await super().startup()
        async with Redis(connection_pool=self.connection_pool) as redis:
            try:
                await redis.xgroup_create(
                    self.stream_name,
                    self.consumer_group_name,
                    id="0",
                    mkstream=True,
                )
            except ResponseError as exc:
                if "BUSYGROUP" not in str(exc):
                    raise
        if self.is_worker_process:
            await self._start_heartbeat()

    async def shutdown(self) -> None:
        """Close Taskiq resources and the Redis connection pool."""
        try:
            await super().shutdown()
        finally:
            await self._stop_heartbeat()
            await self.connection_pool.disconnect()

    async def kick(self, message: BrokerMessage) -> None:
        """Append a Taskiq message to this broker's Redis Stream."""
        async with Redis(connection_pool=self.connection_pool) as redis:
            await redis.xadd(
                self.stream_name,
                {b"data": message.message},
                maxlen=self.maxlen,
                approximate=self.approximate,
            )

    def _available_slots(self, pending: int) -> int | None:
        """Return how many additional PEL entries this listener may reserve."""
        if self.max_pending is None:
            return None
        return max(0, self.max_pending - pending)

    def _reclaim_is_due(self, last_reclaim: float) -> bool:
        """Return whether the next PEL recovery scan should run now."""
        return (
            self.reclaim_interval == 0
            or time.monotonic() - last_reclaim >= self.reclaim_interval / 1_000
        )

    @staticmethod
    def _to_str(value: bytes | str | int) -> str:
        """Normalize Redis's bytes identifiers to strings."""
        return value.decode() if isinstance(value, bytes) else str(value)

    async def _refresh_heartbeat(self) -> None:
        """Renew this consumer's Redis TTL-backed liveness lease."""
        async with Redis(connection_pool=self.connection_pool) as redis:
            await redis.set(
                self.consumer_heartbeat_key,
                b"1",
                px=self.consumer_heartbeat_ttl,
            )

    async def _heartbeat_loop(self) -> None:
        """Keep the consumer lease alive until broker shutdown."""
        while True:
            await asyncio.sleep(self.consumer_heartbeat_interval / 1_000)
            try:
                await self._refresh_heartbeat()
            except asyncio.CancelledError:
                raise
            except RedisError:
                logger.warning(
                    "Unable to renew Redis consumer heartbeat; "
                    "the lease may expire before the next renewal succeeds",
                    exc_info=True,
                )

    async def _start_heartbeat(self) -> None:
        """Create the heartbeat task after its first lease is written."""
        if self._heartbeat_task is not None:
            return
        await self._refresh_heartbeat()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _stop_heartbeat(self) -> None:
        """Stop renewing and remove this consumer's liveness lease."""
        heartbeat_task = self._heartbeat_task
        self._heartbeat_task = None
        if heartbeat_task is not None:
            heartbeat_task.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat_task
        try:
            async with Redis(connection_pool=self.connection_pool) as redis:
                await redis.delete(self.consumer_heartbeat_key)
        except RedisError:
            logger.warning("Unable to remove Redis consumer heartbeat", exc_info=True)

    async def _claim_orphaned_entries(
        self,
        redis: Redis,
        limit: int,
        protected: set[str],
        pending_start: str,
    ) -> tuple[list[StreamEntry], str]:
        """Claim PEL entries whose consumer heartbeat lease has expired."""
        pending = await redis.xpending_range(
            self.stream_name,
            self.consumer_group_name,
            min=pending_start,
            max="+",
            count=self.reclaim_batch_size,
            idle=0,
        )
        pending_entries: list[tuple[str, str]] = []
        owners: list[str] = []
        seen_owners: set[str] = set()
        for pending_entry in pending:
            message_id = self._to_str(pending_entry["message_id"])
            owner = self._to_str(pending_entry["consumer"])
            pending_entries.append((message_id, owner))
            if message_id in protected or owner == self.consumer_name:
                continue
            if owner not in seen_owners:
                seen_owners.add(owner)
                owners.append(owner)

        live_owners: set[str] = set()
        if owners:
            heartbeat_pipeline = redis.pipeline(transaction=False)
            for owner in owners:
                heartbeat_pipeline.exists(
                    consumer_heartbeat_key(
                        self.queue_name,
                        self.namespace,
                        owner,
                    ),
                )
            heartbeat_results = await heartbeat_pipeline.execute()
            live_owners = {
                owner
                for owner, heartbeat_exists in zip(
                    owners,
                    heartbeat_results,
                    strict=True,
                )
                if heartbeat_exists
            }

        claim_candidates: list[tuple[str, str]] = []
        last_checked_id: str | None = None
        for message_id, owner in pending_entries:
            last_checked_id = message_id
            if (
                message_id in protected
                or owner == self.consumer_name
                or owner in live_owners
            ):
                continue
            claim_candidates.append((message_id, owner))
            if len(claim_candidates) >= limit:
                break

        claimed_ids: list[str] = []
        if claim_candidates:
            claim_pipeline = redis.pipeline(transaction=False)
            for message_id, owner in claim_candidates:
                claim_pipeline.eval(
                    _CLAIM_IF_HEARTBEAT_MISSING,
                    2,
                    consumer_heartbeat_key(
                        self.queue_name,
                        self.namespace,
                        owner,
                    ),
                    self.stream_name,
                    self.consumer_group_name,
                    self.consumer_name,
                    self.consumer_heartbeat_ttl,
                    message_id,
                )
            claim_results = await claim_pipeline.execute()
            claimed_ids = [
                self._to_str(cast("bytes | str | int", result))
                for result in claim_results
                if result
            ]

        claimed: list[StreamEntry] = []
        if claimed_ids:
            entries_pipeline = redis.pipeline(transaction=False)
            for message_id in claimed_ids:
                entries_pipeline.xrange(
                    self.stream_name,
                    min=message_id,
                    max=message_id,
                    count=1,
                )
            entries = await entries_pipeline.execute()
            for message_id, entry in zip(claimed_ids, entries, strict=True):
                if entry:
                    claimed.append(
                        (message_id, cast("dict[bytes, bytes]", entry[0][1])),
                    )

        next_start = "-"
        if len(pending) == self.reclaim_batch_size and last_checked_id is not None:
            # XPENDING RANGE's parenthesized lower bound is exclusive. Without
            # it, a live consumer at the head can starve later orphaned entries.
            next_start = f"({last_checked_id}"
        return claimed, next_start

    async def _read_new_entries(
        self,
        redis: Redis,
        count: int | None,
    ) -> list[StreamEntry]:
        """Read fresh Stream entries into this consumer's PEL."""
        fetched = await redis.xreadgroup(
            self.consumer_group_name,
            self.consumer_name,
            {self.stream_name: ">"},
            count=count,
            block=self.xread_block,
        )
        entries: list[StreamEntry] = []
        stream_response = cast("list[tuple[Any, list[tuple[Any, Any]]]]", fetched)
        for _, messages in stream_response:
            for message_id, entry in messages:
                entries.append(
                    (self._to_str(message_id), cast("dict[bytes, bytes]", entry)),
                )
        return entries

    def _ack_callback(
        self,
        message_id: str,
        on_ack: Callable[[], None],
    ) -> Callable[[], Awaitable[None]]:
        """Build an idempotent final ACK callback for one Stream entry."""
        acked = False

        async def ack() -> None:
            nonlocal acked
            if acked:
                return
            async with Redis(connection_pool=self.connection_pool) as redis:
                await redis.xack(
                    self.stream_name,
                    self.consumer_group_name,
                    message_id,
                )
            acked = True
            on_ack()

        return ack

    def _ackable(
        self,
        message_id: str,
        entry: dict[bytes, bytes],
        on_ack: Callable[[], None],
    ) -> AckableMessage:
        """Wrap one Stream entry for Taskiq's acknowledgement lifecycle."""
        return AckableMessage(
            data=entry[b"data"],
            ack=self._ack_callback(message_id, on_ack),
        )

    async def _abandon_buffered_entries(
        self,
        redis: Redis,
        buffered: list[StreamEntry],
    ) -> None:
        """Make fetched but not yielded entries immediately reclaimable on close."""
        if not buffered:
            return
        try:
            await redis.xclaim(
                self.stream_name,
                self.consumer_group_name,
                ABANDONED_CONSUMER,
                min_idle_time=0,
                message_ids=[message_id for message_id, _ in buffered],
                idle=ABANDONED_IDLE_MS,
                justid=True,
            )
        except RedisError:
            # Listener cancellation must not turn a best-effort handoff into a
            # shutdown failure. The next heartbeat-based reclaim scan remains
            # the fallback.
            logger.warning(
                "Unable to abandon buffered Redis Stream entries",
                exc_info=True,
            )

    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        """Yield messages while bounding unacknowledged local prefetch.

        A listener reserves at most ``max_pending`` entries in the Redis PEL.
        Newly read entries are buffered locally and only the entries not yet
        yielded are handed to ``abandoned`` when this generator closes.
        """
        pending = 0
        delivered: set[str] = set()
        buffered: list[StreamEntry] = []
        slot_freed = asyncio.Event()
        last_reclaim = 0.0
        pending_start = "-"

        await self._start_heartbeat()

        def on_ack(message_id: str) -> None:
            nonlocal pending
            delivered.discard(message_id)
            pending = max(0, pending - 1)
            slot_freed.set()

        async with Redis(connection_pool=self.connection_pool) as redis:
            try:
                retry_delay = RETRY_INITIAL_DELAY
                while True:
                    try:
                        available = self._available_slots(pending)
                        if available == 0:
                            await slot_freed.wait()
                            slot_freed.clear()
                            continue

                        if self._reclaim_is_due(last_reclaim):
                            reclaim_limit = available or self.reclaim_batch_size
                            reclaim_result = await self._claim_orphaned_entries(
                                redis,
                                reclaim_limit,
                                delivered,
                                pending_start,
                            )
                            buffered, pending_start = reclaim_result
                            last_reclaim = time.monotonic()

                        if not buffered:
                            available = self._available_slots(pending)
                            read_count = (
                                self.xread_count
                                if available is None
                                else min(self.xread_count, available)
                            )
                            buffered = await self._read_new_entries(redis, read_count)

                        pending += len(buffered)
                        while buffered:
                            message_id, entry = buffered.pop(0)
                            delivered.add(message_id)

                            def acknowledge(message_id: str = message_id) -> None:
                                on_ack(message_id)

                            yield self._ackable(
                                message_id,
                                entry,
                                acknowledge,
                            )
                        retry_delay = RETRY_INITIAL_DELAY
                    except asyncio.CancelledError:
                        raise
                    except RedisError:
                        logger.warning(
                            "Redis error while listening; retrying in %.1f seconds",
                            retry_delay,
                            exc_info=True,
                        )
                        await asyncio.sleep(retry_delay)
                        retry_delay = min(retry_delay * 2, RETRY_MAX_DELAY)
            finally:
                await self._abandon_buffered_entries(redis, buffered)
