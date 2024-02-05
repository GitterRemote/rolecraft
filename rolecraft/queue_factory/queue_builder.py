from typing import Sequence, TypedDict, Unpack

from rolecraft.broker import Broker
from rolecraft.queue import MessageQueue, Middleware

from .config_fetcher import ConfigFetcher, QueueConfig, QueueConfigOptions


class QueueAndNameKeys(TypedDict, total=False):
    queue_names: list[str] | None
    queue_names_by_broker: dict[Broker, list[str]] | None
    raw_queues: list[MessageQueue] | None


class QueueBuildOptions(TypedDict, total=False):
    ensure_queue: bool


class QueueBuilder:
    """Build and decorate queues according to the queue config."""

    def __init__(
        self, config_fetcher: ConfigFetcher, options: QueueBuildOptions
    ):
        self.config_fetcher = config_fetcher
        self.options = options

    def build_queue(self, queue_name: str, **kwds: Unpack[QueueConfigOptions]):
        return self._build_queue(queue_name, **kwds)

    def build(self, **kwds: Unpack[QueueAndNameKeys]) -> list[MessageQueue]:
        all_queues: list[MessageQueue] = []

        if queue_names := kwds.get("queue_names"):
            all_queues.extend(self._build_queues(queue_names))

        if queue_names_by_broker := kwds.get("queue_names_by_broker"):
            for broker, queue_names in queue_names_by_broker.items():
                all_queues.extend(
                    self._build_queues(queue_names, broker=broker)
                )

        if raw_queues := kwds.get("raw_queues"):
            all_queues.extend(map(self.setup_queue, raw_queues))

        return all_queues

    def _build_queues(self, queue_names, **kwds: Unpack[QueueConfigOptions]):
        for queue_name in queue_names:
            yield self._build_queue(queue_name, **kwds)

    def _build_queue(
        self, queue_name: str, **kwds: Unpack[QueueConfigOptions]
    ):
        config = self.config_fetcher(queue_name, **kwds)
        queue = self._new_queue(queue_name, config)
        return self._setup_queue(queue, config)

    def setup_queue(self, raw_queue: MessageQueue) -> MessageQueue:
        config = self.config_fetcher(raw_queue.name, broker=raw_queue.broker)
        return self._setup_queue(raw_queue, config)

    def _setup_queue(
        self, raw_queue: MessageQueue, queue_config: QueueConfig
    ) -> MessageQueue:
        wrapped = self._wrap(raw_queue, queue_config.middlewares)
        wrapped.prepare(ensure=self.options.get("ensure_queue", False))
        return wrapped

    def _wrap(
        self, raw_queue: MessageQueue, middlewares: Sequence[Middleware]
    ) -> MessageQueue:
        queue = raw_queue
        for middleware in middlewares:
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
            settings=config.settings,
        )
