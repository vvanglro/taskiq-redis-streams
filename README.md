# taskiq-redis-streams

An independent, single-node Redis Streams broker for
[Taskiq](https://taskiq-python.github.io/). It uses bounded local prefetch and
timeout-aware pending-entry recovery.

Redis Streams delivery is at least once. A task can execute more than once
after a worker crash or failed acknowledgement, so handlers must be idempotent.

## Installation

```bash
uv add taskiq-redis-streams
```

## Usage

```python
from taskiq_redis_streams import RedisStreamsBroker

broker = RedisStreamsBroker(
    "redis://localhost:6379/0",
    queue_name="default",
    namespace="my-service",
)


@broker.task
async def process_order(order_id: str) -> None:
    # Make this operation idempotent.
    ...
```

```bash
taskiq worker my_app:broker
```

The group starts at `0` by default, so tasks published before the first worker
starts are consumed. Pass `consumer_id="$"` to read only future messages.

## Behavior

One broker instance serves one Taskiq queue and uses these namespaced keys:

```text
<namespace>:stream:<queue_name>
<namespace>:workers:<queue_name>
```

Use a distinct `namespace` for applications that share Redis. The default is
`taskiq`.

`xread_count` controls a single `XREADGROUP` batch. `max_pending` independently
caps entries fetched by a listener but not successfully acknowledged; both
default to `100`. Once the cap is reached, the listener leaves new work for
other consumers. Set `max_pending=None` to disable that local cap.

The broker scans the consumer group's PEL periodically. A serialized Taskiq
message with a `timeout` label is reclaimable after:

```text
timeout * 1000 + reclaim_timeout_grace
```

Messages without a readable timeout use `reclaim_timeout`. `XCLAIM` performs
the final deadline check atomically in Redis, so no distributed reclaim lock is
needed. Reclaimed entries are handled before new entries when a scan is due.

Set `reclaim_enabled=False` to disable all automatic PEL recovery. In this
mode, the broker neither scans pending entries nor hands buffered entries to
the `abandoned` consumer when a listener closes. This is useful when the
application owns recovery policy for long-running tasks. Unacknowledged tasks
must then be recovered explicitly with Redis commands or a broker configured
with reclaim enabled.

On listener close, only messages still in the broker-local buffer are handed to
an internal `abandoned` consumer and become reclaimable immediately. Already
yielded messages may be executing, so they follow the normal acknowledge or
reclaim lifecycle.

Redis Cluster, Sentinel, delayed tasks, and a stream-level dead-letter queue
are not part of the first release.

## Development

Tests clear a dedicated Redis database before and after every test:

```bash
TEST_REDIS_URL=redis://127.0.0.1:7000/14 uv run pytest -q
```

Do not point `TEST_REDIS_URL` at a database containing application data.

```bash
uv sync --all-groups
uv run ruff check .
uv run mypy
uv run pytest -q
```

## Inspiration

The Taskiq integration follows ecosystem patterns from
[taskiq-redis](https://github.com/taskiq-python/taskiq-redis). Recovery, local
prefetch, and shutdown handoff are informed by
[dramatiq-redis-streams](https://github.com/sylvinus/dramatiq-redis-streams).

This project is independently maintained and is not affiliated with either
upstream project.
