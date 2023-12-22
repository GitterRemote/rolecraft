from .queue import Queue
from .config import Config, ConfigFetcher
from .broker import Broker, default_broker


class QueueBuilder:
    """Build and decorate queues according to the queue config."""

    def __init__(
        self,
        config_fetcher: ConfigFetcher,
        queue_names: list[str] | None = None,
        queues: list[Queue] | None = None,
    ):
        self.queue_names = queue_names
        self.queues = queues
        self.config_fetcher = config_fetcher

    def build(self) -> list[Queue]:
        return self._build_queues(self.queue_names, self.queues)

    def _build_queues(self, queue_names, queues) -> list[Queue]:
        all_queues = []

        if queue_names:
            all_queues.extend(
                self._build_queues_with_default_broker(queue_names)
            )

        all_queues.extend(queues)

        if not all_queues:
            all_queues.append(self._get_default_queue())

        return [self._wrap(q) for q in all_queues]

    def _build_queues_with_default_broker(self, queue_names):
        broker = self._get_default_broker()
        for queue_name in queue_names:
            config = self.config_fetcher(queue_name, broker)
            yield self._new_queue(queue_name, broker, config)

    def _get_default_broker(self) -> Broker:
        assert default_broker
        return default_broker

    def _wrap(self, queue: Queue) -> Queue:
        config = self.config_fetcher(queue.name, queue.broker)
        for middleware in config.middlewares:
            queue = middleware(queue)
            assert isinstance(queue, Queue)
        return queue

    def _get_default_queue(self) -> Queue:
        return self._new_queue(
            name="default",
            broker=self._get_default_broker(),
            config=Config.default(),
        )

    def _new_queue(self, name: str, broker: Broker, config: Config) -> Queue:
        return Queue(
            name=name,
            broker=broker,
            encoder=config.encoder,
            wait_time_seconds=config.consumer_wait_time_seconds,
        )
