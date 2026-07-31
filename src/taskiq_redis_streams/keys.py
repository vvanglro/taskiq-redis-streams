"""Redis key naming helpers."""

from __future__ import annotations


def stream_key(queue_name: str, namespace: str) -> str:
    """Return the Redis Stream key for a Taskiq queue."""
    return f"{namespace}:stream:{queue_name}"


def consumer_group_key(queue_name: str, namespace: str) -> str:
    """Return the default consumer group key for a Taskiq queue."""
    return f"{namespace}:workers:{queue_name}"
