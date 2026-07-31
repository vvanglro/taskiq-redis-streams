"""Redis Streams broker with bounded local prefetch and timeout-aware recovery."""

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

from taskiq_redis_streams.keys import consumer_group_key, stream_key

logger = getLogger(__name__)

ABANDONED_CONSUMER = "abandoned"
ABANDONED_IDLE_MS = 10**12
RETRY_INITIAL_DELAY = 0.1
RETRY_MAX_DELAY = 5.0

StreamEntry: TypeAlias = tuple[str, dict[bytes, bytes]]
ConnectionPool: TypeAlias = BlockingConnectionPool


class RedisStreamsBroker(AsyncBroker):
    """A single-queue Taskiq broker backed by one Redis Stream.

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
        reclaim_enabled: bool = True,
        reclaim_timeout: int = 600_000,
        reclaim_timeout_grace: int = 10_000,
        reclaim_interval: int = 30_000,
        reclaim_batch_size: int = 100,
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
        :param reclaim_enabled: Enable automatic recovery of pending entries.
            Set to ``False`` when recovery is managed outside this broker.
        :param reclaim_timeout: Fallback reclaim deadline in milliseconds for
            payloads without a valid Taskiq ``timeout`` label.
        :param reclaim_timeout_grace: Extra milliseconds added to a task's
            timeout label before an unacknowledged entry is reclaimed.
        :param reclaim_interval: Milliseconds between PEL recovery scans. Set
            to ``0`` to scan before every read.
        :param reclaim_batch_size: Maximum PEL entries examined in one scan.
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
        if reclaim_timeout < 0 or reclaim_timeout_grace < 0:
            raise ValueError("reclaim timeouts must be non-negative")
        if reclaim_interval < 0:
            raise ValueError("reclaim_interval must be non-negative")
        if reclaim_batch_size <= 0:
            raise ValueError("reclaim_batch_size must be greater than zero")

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
        self.xread_block = xread_block
        self.xread_count = xread_count
        self.max_pending = max_pending
        self.maxlen = maxlen
        self.approximate = approximate
        self.reclaim_enabled = reclaim_enabled
        self.reclaim_timeout = reclaim_timeout
        self.reclaim_timeout_grace = reclaim_timeout_grace
        self.reclaim_interval = reclaim_interval
        self.reclaim_batch_size = reclaim_batch_size

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

    async def shutdown(self) -> None:
        """Close Taskiq resources and the Redis connection pool."""
        await super().shutdown()
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

    def _reclaim_deadline(self, entry: dict[bytes, bytes]) -> int:
        """Resolve an entry's reclaim deadline from its serialized task label."""
        payload = entry.get(b"data")
        if payload is None:
            return self.reclaim_timeout

        with suppress(Exception):
            timeout = self.formatter.loads(payload).labels.get("timeout")
            if timeout is not None:
                return int(float(timeout) * 1_000) + self.reclaim_timeout_grace
        return self.reclaim_timeout

    async def _entry_for_pending_id(
        self,
        redis: Redis,
        message_id: str,
    ) -> dict[bytes, bytes] | None:
        """Load one PEL entry from the Stream so its task timeout can be read."""
        entries = await redis.xrange(
            self.stream_name,
            min=message_id,
            max=message_id,
            count=1,
        )
        if not entries:
            return None
        return cast("dict[bytes, bytes]", entries[0][1])

    async def _claim_overdue_entries(
        self,
        redis: Redis,
        limit: int,
        protected: set[str],
        pending_start: str,
    ) -> tuple[list[StreamEntry], str]:
        """Claim overdue PEL entries, with Redis enforcing the final deadline.

        ``XCLAIM min_idle_time`` is the atomic guard: concurrent workers may
        inspect the same PEL entry, but only one can successfully claim it.
        """
        pending = await redis.xpending_range(
            self.stream_name,
            self.consumer_group_name,
            min=pending_start,
            max="+",
            count=self.reclaim_batch_size,
            idle=0,
        )
        claimed: list[StreamEntry] = []
        last_checked_id: str | None = None

        for pending_entry in pending:
            if len(claimed) >= limit:
                break

            message_id = self._to_str(pending_entry["message_id"])
            last_checked_id = message_id
            if message_id in protected:
                continue

            entry = await self._entry_for_pending_id(redis, message_id)
            if entry is None:
                # Redis removes a PEL reference when XCLAIM finds that the
                # matching Stream entry has already been trimmed or deleted.
                await redis.xclaim(
                    self.stream_name,
                    self.consumer_group_name,
                    self.consumer_name,
                    min_idle_time=0,
                    message_ids=[message_id],
                    justid=True,
                )
                continue

            deadline = self._reclaim_deadline(entry)
            idle_time = int(cast(Any, pending_entry["time_since_delivered"]))
            if idle_time < deadline:
                continue

            result = await redis.xclaim(
                self.stream_name,
                self.consumer_group_name,
                self.consumer_name,
                min_idle_time=deadline,
                message_ids=[message_id],
            )
            for claimed_id, claimed_entry in cast("list[Any]", result):
                claimed.append(
                    (
                        self._to_str(cast("bytes | str", claimed_id)),
                        cast("dict[bytes, bytes]", claimed_entry),
                    ),
                )

        next_start = "-"
        if len(pending) == self.reclaim_batch_size and last_checked_id is not None:
            # XPENDING RANGE's parenthesized lower bound is exclusive. Without
            # it, a protected head entry can starve later abandoned entries.
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
            # shutdown failure. The normal reclaim timeout remains the fallback.
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

                        if self.reclaim_enabled and self._reclaim_is_due(last_reclaim):
                            reclaim_limit = available or self.reclaim_batch_size
                            buffered, pending_start = await self._claim_overdue_entries(
                                redis,
                                reclaim_limit,
                                delivered,
                                pending_start,
                            )
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
                if self.reclaim_enabled:
                    await self._abandon_buffered_entries(redis, buffered)
