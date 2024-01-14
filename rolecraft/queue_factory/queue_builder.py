from typing import TypedDict, Unpack

from rolecraft.broker import Broker
from rolecraft.queue import MessageQueue

from .config_fetcher import ConfigFetcher, QueueConfig, QueueConfigOptions


class QueueBuildOptions(TypedDict, total=False):
    queue_names: list[str] | None
    queue_names_by_broker: dict[Broker, list[str]] | None
    raw_queues: list[MessageQueue] | None
    prepare: bool


class QueueBuilder:
    """Build and decorate queues according to the queue config."""

    def __init__(self, config_fetcher: ConfigFetcher):
        self.config_fetcher = config_fetcher

    def build_one(self, queue_name: str, **kwds: Unpack[QueueConfigOptions]):
        return self._build_queue(queue_name, **kwds)

    def build(self, **kwds: Unpack[QueueBuildOptions]) -> list[MessageQueue]:
        all_queues: list[MessageQueue] = []

        if queue_names := kwds.get("queue_names"):
            all_queues.extend(self._build_queues(queue_names))

        if queue_names_by_broker := kwds.get("queue_names_by_broker"):
            for broker, queue_names in queue_names_by_broker.items():
                all_queues.extend(
                    self._build_queues(queue_names, broker=broker)
                )

        if raw_queues := kwds.get("raw_queues"):
            all_queues.extend(raw_queues)  # FIXME: wrap it

        if kwds.get("prepare", True):
            for queue in all_queues:
                queue.prepare()

        return all_queues

    def _build_queues(self, queue_names, **kwds: Unpack[QueueConfigOptions]):
        for queue_name in queue_names:
            yield self._build_queue(queue_name, **kwds)

    def _build_queue(
        self, queue_name: str, **kwds: Unpack[QueueConfigOptions]
    ):
        config = self.config_fetcher(queue_name, **kwds)
        return self._build_queue_with_config(queue_name, config)

    def _build_queue_with_config(self, queue_name: str, config: QueueConfig):
        queue = self._new_queue(queue_name, config)
        return self._wrap(queue, config)

    def wrap(self, raw_queue: MessageQueue) -> MessageQueue:
        config = self.config_fetcher(raw_queue.name, broker=raw_queue.broker)
        return self._wrap(raw_queue, config)

    def _wrap(
        self, raw_queue: MessageQueue, config: QueueConfig
    ) -> MessageQueue:
        queue = raw_queue
        for middleware in config.middlewares:
            queue = middleware(queue)
            assert isinstance(queue, MessageQueue)
        return queue

    def _new_queue[M](
        self, name: str, config: QueueConfig[M]
    ) -> MessageQueue[M]:
        return MessageQueue(
            name=name,
            broker=config.broker,
            encoder=config.encoder,
            wait_time_seconds=config.wait_time_seconds,
        )
