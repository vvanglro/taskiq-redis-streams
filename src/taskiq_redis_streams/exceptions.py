"""Exceptions raised by the Redis result backend."""

from taskiq.exceptions import ResultBackendError, ResultGetError, TaskiqError


class TaskIQRedisStreamsError(TaskiqError):
    """Base error for taskiq-redis-streams exceptions."""


class DuplicateExpireTimeSelectedError(ResultBackendError, TaskIQRedisStreamsError):
    """Raised when both result expiration units are configured."""

    __template__ = "Choose either result_ex_time or result_px_time."


class ExpireTimeMustBeMoreThanZeroError(ResultBackendError, TaskIQRedisStreamsError):
    """Raised when a configured result expiration is non-positive."""

    __template__ = (
        "You must select one expire time param and it must be more than zero."
    )


class ResultIsMissingError(TaskIQRedisStreamsError, ResultGetError):
    """Raised when a requested Redis result does not exist."""
