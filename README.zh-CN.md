# taskiq-redis-streams

[English](README.md)

一个独立维护的单节点 Redis Streams [Taskiq](https://taskiq-python.github.io/)
broker，提供受限的本地预取和基于任务时限的 PEL 恢复机制。

Redis Streams 提供的是至少一次投递语义。worker 崩溃或确认失败后，任务可能
会执行多次，因此任务处理函数必须是幂等的。

## 安装

```bash
uv add taskiq-redis-streams
```

## 使用方法

```python
from taskiq_redis_streams import RedisStreamsBroker

broker = RedisStreamsBroker(
    "redis://localhost:6379/0",
    queue_name="default",
    namespace="my-service",
)


@broker.task
async def process_order(order_id: str) -> None:
    # 此操作需要保持幂等。
    ...
```

```bash
taskiq worker my_app:broker
```

默认的 consumer group 从 stream offset `0` 开始，因此第一个 worker 启动前
已经发布的任务也会被消费。传入 `consumer_id="$"` 则只读取之后发布的任务。

## 行为说明

每个 broker 实例服务于一个 Taskiq queue，并使用以下带命名空间的 Redis key：

```text
<namespace>:stream:<queue_name>
<namespace>:workers:<queue_name>
```

多个应用共用同一个 Redis 时应使用不同的 `namespace`。默认值为 `taskiq`。

`xread_count` 控制一次 `XREADGROUP` 拉取的消息数量。`max_pending` 独立限制
一个 listener 已拉取但尚未成功确认的 entry 数量，二者默认都是 `100`。达到
上限后，listener 不会继续占用新的 stream entry，从而让同一 group 中的其他
consumer 有机会获取任务。设置 `max_pending=None` 可关闭这一项本地限制。

默认情况下，确认会将 entry 从 PEL 移除，但仍保留在 Stream 中。设置
`delete_after_ack=True` 后，会在成功 `XACK` 后执行 `XDEL`，以减少历史消息的
存储。删除后的 entry 无法检查或重放；若仍希望保留最近的历史，优先使用
`maxlen`。

broker 会定期扫描 consumer group 的 pending entries list (PEL)。对于包含
`timeout` label 的序列化 Taskiq 消息，entry 在以下时间后可以被 reclaim：

```text
timeout * 1000 + reclaim_timeout_grace
```

无法读取 timeout 的消息使用 `reclaim_timeout`。`XCLAIM` 会在 Redis 内原子地
完成最终的 deadline 校验，因此不需要额外的分布式 reclaim 锁。reclaim scan
到期时，已恢复的 entry 会优先于新消息处理。

设置 `reclaim_enabled=False` 可以关闭所有自动 PEL 恢复。在该模式下，broker
既不扫描 pending entry，也不会在 listener 关闭时将缓冲 entry 交接给内部的
`abandoned` consumer。这适用于应用需要自行管理长时间任务恢复策略的场景。
此时未确认的任务必须由用户通过 Redis 命令，或启动一个开启 reclaim 的 broker
来显式恢复。

listener 关闭时，只有仍在 broker 本地 buffer 内、尚未 yield 给 Taskiq 的消息
会被交接给内部 `abandoned` consumer，并立即具备被下次 recovery scan 恢复的
资格。已经 yield 的消息可能正在执行，因此仍遵循正常的确认或 reclaim 生命周期。

首个版本暂不支持 Redis Cluster、Sentinel、延迟任务和 stream 级死信队列。

## 开发

测试会在每个测试前后清空指定的 Redis database：

```bash
TEST_REDIS_URL=redis://127.0.0.1:7000/14 uv run pytest -q
```

不要将 `TEST_REDIS_URL` 指向包含应用数据的 database。

```bash
uv sync --all-groups
uv run ruff check .
uv run mypy
uv run pytest -q
```

## 参考

Taskiq 集成遵循 [taskiq-redis](https://github.com/taskiq-python/taskiq-redis)
的生态模式。任务恢复、本地预取和关闭时消息交接的设计参考了
[dramatiq-redis-streams](https://github.com/sylvinus/dramatiq-redis-streams)。

本项目独立维护，与上述两个上游项目没有隶属关系。
