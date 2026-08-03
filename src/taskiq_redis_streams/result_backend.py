"""Redis-backed Taskiq result storage."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeAlias, TypeVar, cast

from redis.asyncio import BlockingConnectionPool, Redis
from redis.asyncio.connection import Connection
from taskiq import AsyncResultBackend
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.compat import model_dump, model_validate
from taskiq.depends.progress_tracker import TaskProgress
from taskiq.result import TaskiqResult
from taskiq.serializers import PickleSerializer

from taskiq_redis_streams.exceptions import (
    DuplicateExpireTimeSelectedError,
    ExpireTimeMustBeMoreThanZeroError,
    ResultIsMissingError,
)

if TYPE_CHECKING:
    RedisPool: TypeAlias = BlockingConnectionPool[Connection]  # type: ignore
else:
    RedisPool: TypeAlias = BlockingConnectionPool

_ReturnType = TypeVar("_ReturnType")
PROGRESS_KEY_SUFFIX = "__progress"


class RedisAsyncResultBackend(AsyncResultBackend[_ReturnType]):
    """Store Taskiq task results and progress in Redis."""

    def __init__(
        self,
        redis_url: str,
        keep_results: bool = True,
        result_ex_time: int | None = None,
        result_px_time: int | None = None,
        max_connection_pool_size: int | None = None,
        serializer: TaskiqSerializer | None = None,
        prefix_str: str | None = None,
        **connection_kwargs: Any,
    ) -> None:
        """Create a Redis result backend.

        :param redis_url: Redis connection URL.
        :param keep_results: Keep a result after it is read when set.
        :param result_ex_time: Result and progress expiration in seconds.
        :param result_px_time: Result and progress expiration in milliseconds.
        :param max_connection_pool_size: Maximum Redis connections in the pool.
        :param serializer: Serializer used to encode Taskiq result models.
        :param prefix_str: Optional prefix prepended to every result key.
        :param connection_kwargs: Extra arguments accepted by redis-py.
        :raises DuplicateExpireTimeSelectedError: If both expiration units are
            configured.
        :raises ExpireTimeMustBeMoreThanZeroError: If an expiration is not
            positive.
        """
        if (
            result_ex_time is not None
            and result_ex_time <= 0
            or result_px_time is not None
            and result_px_time <= 0
        ):
            raise ExpireTimeMustBeMoreThanZeroError
        if result_ex_time is not None and result_px_time is not None:
            raise DuplicateExpireTimeSelectedError

        connection_kwargs["decode_responses"] = False
        self.redis_pool: RedisPool = BlockingConnectionPool.from_url(
            redis_url,
            max_connections=max_connection_pool_size,
            **connection_kwargs,
        )
        self.serializer = serializer or PickleSerializer()
        self.keep_results = keep_results
        self.result_ex_time = result_ex_time
        self.result_px_time = result_px_time
        self.prefix_str = prefix_str

    def _task_name(self, task_id: str) -> str:
        """Return the Redis key used for a Taskiq task ID."""
        if self.prefix_str is None:
            return task_id
        return f"{self.prefix_str}:{task_id}"

    async def shutdown(self) -> None:
        """Close the Redis connection pool."""
        await self.redis_pool.disconnect()
        await super().shutdown()

    async def _set_value(self, name: str, value: bytes) -> None:
        """Write one serialized result value with the configured expiration."""
        async with Redis(connection_pool=self.redis_pool) as redis:
            if self.result_ex_time is not None:
                await redis.set(name=name, value=value, ex=self.result_ex_time)
            elif self.result_px_time is not None:
                await redis.set(name=name, value=value, px=self.result_px_time)
            else:
                await redis.set(name=name, value=value)

    async def set_result(
        self,
        task_id: str,
        result: TaskiqResult[_ReturnType],
    ) -> None:
        """Serialize and save a Taskiq task result."""
        await self._set_value(
            self._task_name(task_id),
            self.serializer.dumpb(model_dump(result)),
        )

    async def is_result_ready(self, task_id: str) -> bool:
        """Return whether Redis contains a result for the task."""
        async with Redis(connection_pool=self.redis_pool) as redis:
            return bool(await redis.exists(self._task_name(task_id)))

    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> TaskiqResult[_ReturnType]:
        """Load one result, optionally consuming it after the read."""
        task_name = self._task_name(task_id)
        async with Redis(connection_pool=self.redis_pool) as redis:
            if self.keep_results:
                result_value = await redis.get(task_name)
            else:
                result_value = await redis.getdel(task_name)

        if result_value is None:
            raise ResultIsMissingError

        taskiq_result = model_validate(
            TaskiqResult[_ReturnType],
            self.serializer.loadb(cast("bytes", result_value)),
        )
        if not with_logs:
            taskiq_result.log = None
        return taskiq_result

    async def set_progress(
        self,
        task_id: str,
        progress: TaskProgress[_ReturnType],
    ) -> None:
        """Serialize and save Taskiq progress with the result expiration."""
        await self._set_value(
            self._task_name(task_id) + PROGRESS_KEY_SUFFIX,
            self.serializer.dumpb(model_dump(progress)),
        )

    async def get_progress(
        self,
        task_id: str,
    ) -> TaskProgress[_ReturnType] | None:
        """Load saved task progress, or return ``None`` when it is absent."""
        async with Redis(connection_pool=self.redis_pool) as redis:
            progress_value = await redis.get(
                self._task_name(task_id) + PROGRESS_KEY_SUFFIX,
            )
        if progress_value is None:
            return None
        return model_validate(
            TaskProgress[_ReturnType],
            self.serializer.loadb(cast("bytes", progress_value)),
        )
