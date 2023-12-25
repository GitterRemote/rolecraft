import typing

from .broker import Broker
from .config import ConfigFetcher, QueueConfig
from .queue import MessageQueue


class QueueBuilder:
    """Build and decorate queues according to the queue config."""

    def __init__(
        self,
        config_fetcher: ConfigFetcher,
    ):
        self.config_fetcher = config_fetcher

    def build(
        self,
        queue_names: list[str] | None = None,
        queue_names_per_broker: dict[Broker, list[str]] | None = None,
        raw_queues: list[MessageQueue] | None = None,
    ) -> list[MessageQueue]:
        return self._build_queues(
            queue_names, queue_names_per_broker, raw_queues
        )

    def _build_queues(
        self, queue_names, queue_names_per_broker, raw_queues
    ) -> list[MessageQueue]:
        all_queues = []

        if queue_names:
            all_queues.extend(self._build_raw_queues(queue_names))

        if queue_names_per_broker:
            for broker, queue_names in queue_names_per_broker.items():
                all_queues.extend(self._build_raw_queues(queue_names, broker))

        all_queues.extend(raw_queues)

        if not all_queues:
            all_queues.append(self._get_default_queue())

        return [self._wrap(q) for q in all_queues]

    def _build_raw_queues(self, queue_names, broker: Broker | None = None):
        for queue_name in queue_names:
            config = self.config_fetcher(queue_name, broker=broker)
            yield self._new_queue(queue_name, config)

    def _wrap(self, queue: MessageQueue) -> MessageQueue:
        config = self.config_fetcher(queue.name, broker=queue.broker)
        assert config.middlewares
        for middleware in config.middlewares:
            queue = typing.cast(MessageQueue, middleware(queue))
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
