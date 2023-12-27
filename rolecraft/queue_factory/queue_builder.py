from typing import TypedDict, Unpack

from rolecraft.broker import Broker
from rolecraft.queue import MessageQueue

from .config_fetcher import QueueConfigOptions, ConfigFetcher, QueueConfig


class QueueAndNameKeys(TypedDict, total=False):
    queue_names: list[str] | None
    queue_names_by_broker: dict[Broker, list[str]] | None
    raw_queues: list[MessageQueue] | None


class QueueBuilder:
    """Build and decorate queues according to the queue config."""

    def __init__(
        self,
        config_fetcher: ConfigFetcher,
    ):
        self.config_fetcher = config_fetcher

    def build_one(self, queue_name: str, **kwds: Unpack[QueueConfigOptions]):
        raw_queue = self._build_raw_queue(queue_name, **kwds)
        return self.wrap(raw_queue)

    def build(self, **kwds: Unpack[QueueAndNameKeys]) -> list[MessageQueue]:
        return self._build_queues(**kwds)

    def _build_queues(
        self, queue_names, queue_names_by_broker, raw_queues
    ) -> list[MessageQueue]:
        all_queues = []

        if queue_names:
            all_queues.extend(self._build_raw_queues(queue_names))

        if queue_names_by_broker:
            for broker, queue_names in queue_names_by_broker.items():
                all_queues.extend(
                    self._build_raw_queues(queue_names, broker=broker)
                )

        all_queues.extend(raw_queues)

        if not all_queues:
            all_queues.append(self._get_default_queue())

        return [self.wrap(q) for q in all_queues]

    def _build_raw_queues(
        self, queue_names, **kwds: Unpack[QueueConfigOptions]
    ):
        for queue_name in queue_names:
            yield self._build_raw_queue(queue_name, **kwds)

    def _build_raw_queue(
        self, queue_name: str, **kwds: Unpack[QueueConfigOptions]
    ):
        config = self.config_fetcher(queue_name, **kwds)
        return self._new_queue(queue_name, config)

    def wrap(self, raw_queue: MessageQueue) -> MessageQueue:
        config = self.config_fetcher(raw_queue.name, broker=raw_queue.broker)
        assert config.middlewares is not None
        queue = raw_queue
        for middleware in config.middlewares:
            queue = middleware(queue)
            assert isinstance(queue, MessageQueue)
        return queue

    def _get_default_queue(self) -> MessageQueue:
        default_config = self.config_fetcher()
        return self._new_queue(name="default", config=default_config)

    def _new_queue[M](
        self, name: str, config: QueueConfig[M]
    ) -> MessageQueue[M]:
        return MessageQueue(
            name=name,
            broker=config.broker,
            encoder=config.encoder,
            wait_time_seconds=config.consumer_wait_time_seconds,
        )
