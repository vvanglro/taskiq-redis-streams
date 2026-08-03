"""Integration coverage for the Redis result backend."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest
from taskiq.depends.progress_tracker import TaskProgress
from taskiq.result import TaskiqResult

from taskiq_redis_streams import RedisAsyncResultBackend
from taskiq_redis_streams.exceptions import (
    DuplicateExpireTimeSelectedError,
    ExpireTimeMustBeMoreThanZeroError,
    ResultIsMissingError,
)


def task_result(log: str | None = None) -> TaskiqResult[str]:
    """Build a serializable result for Redis backend tests."""
    return TaskiqResult(
        is_err=False,
        log=log,
        return_value="complete",
        execution_time=0.1,
    )


@pytest.mark.asyncio
async def test_result_backend_round_trip_and_prefix(redis_url: str) -> None:
    """Results are isolated by prefix and retain logs when requested."""
    backend: RedisAsyncResultBackend[Any] = RedisAsyncResultBackend(
        redis_url,
        prefix_str="test:result",
    )
    task_id = uuid.uuid4().hex
    try:
        await backend.set_result(task_id, task_result(log="task log"))

        assert await backend.is_result_ready(task_id)
        assert await backend.get_result(task_id) == task_result()
        assert await backend.get_result(task_id, with_logs=True) == task_result(
            log="task log",
        )
    finally:
        await backend.shutdown()


@pytest.mark.asyncio
async def test_result_backend_consumes_results_when_configured(
    redis_url: str,
) -> None:
    """A non-retaining backend removes a result atomically after reading it."""
    backend: RedisAsyncResultBackend[Any] = RedisAsyncResultBackend(
        redis_url,
        keep_results=False,
    )
    task_id = uuid.uuid4().hex
    try:
        await backend.set_result(task_id, task_result())
        assert await backend.get_result(task_id) == task_result()
        assert not await backend.is_result_ready(task_id)
        with pytest.raises(ResultIsMissingError):
            await backend.get_result(task_id)
    finally:
        await backend.shutdown()


@pytest.mark.asyncio
async def test_result_backend_expires_results_and_progress(redis_url: str) -> None:
    """The configured millisecond expiration applies to both Redis keys."""
    backend: RedisAsyncResultBackend[Any] = RedisAsyncResultBackend(
        redis_url,
        result_px_time=20,
    )
    task_id = uuid.uuid4().hex
    progress = TaskProgress(state="running", meta={"completed": 1})
    try:
        await backend.set_result(task_id, task_result())
        await backend.set_progress(task_id, progress)
        assert await backend.get_progress(task_id) == progress

        await asyncio.sleep(0.05)
        assert not await backend.is_result_ready(task_id)
        assert await backend.get_progress(task_id) is None
    finally:
        await backend.shutdown()


@pytest.mark.parametrize(
    ("result_ex_time", "result_px_time", "error"),
    [
        (1, 1, DuplicateExpireTimeSelectedError),
        (0, None, ExpireTimeMustBeMoreThanZeroError),
        (None, 0, ExpireTimeMustBeMoreThanZeroError),
    ],
)
def test_result_backend_validates_expiration_options(
    result_ex_time: int | None,
    result_px_time: int | None,
    error: type[Exception],
) -> None:
    """Only one positive result expiration unit may be selected."""
    with pytest.raises(error):
        RedisAsyncResultBackend(
            "redis://127.0.0.1:7000/14",
            result_ex_time=result_ex_time,
            result_px_time=result_px_time,
        )
