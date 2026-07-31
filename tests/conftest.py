"""Shared Redis test fixtures."""

from __future__ import annotations

import os
from collections.abc import AsyncGenerator

import pytest_asyncio
from redis.asyncio import Redis


@pytest_asyncio.fixture
async def redis_url() -> AsyncGenerator[str, None]:
    """Provide an empty, dedicated Redis database for each test."""
    url = os.environ.get("TEST_REDIS_URL", "redis://127.0.0.1:7000/14")
    redis = Redis.from_url(url)
    await redis.flushdb()
    try:
        yield url
    finally:
        await redis.flushdb()
        await redis.aclose()
