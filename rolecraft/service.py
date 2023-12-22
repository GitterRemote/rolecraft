import logging
import signal
from .queue import MessageQueue
from .config import Config, ConfigFetcher, DefaultConfigFetcher
from .broker import Broker
from . import queue_builder as _queue_builder
from . import worker_pool as _worker_pool
from . import consumer as _consumer
from . import worker as _worker

logger = logging.getLogger(__name__)


class Service:
    def __init__(
        self,
        *,
        queue_names: list[str] | None = None,
        queues: list[MessageQueue] | None = None,
        config: Config | None = None,
        queue_configs: dict[str, Config] | None = None,
        broker_configs: dict[Broker, Config] | None = None,
        config_fetcher: ConfigFetcher | None = None,
    ) -> None:
        # TODO: move into start method
        self.config_fetcher = config_fetcher or DefaultConfigFetcher(
            config, queue_configs, broker_configs
        )

        self.queues = _queue_builder.QueueBuilder(
            config_fetcher=self.config_fetcher,
            queue_names=queue_names,
            queues=queues,
        ).build()

        self.worker_pool = _worker_pool.ThreadWorkerPool()
        self.consumer: _consumer.Consumer = _consumer.DefaultConsumer(
            queues=self.queues
        )
        self.worker = _worker.Worker(worker_pool=self.worker_pool)

    def start(self, *, thread_num: int):
        """
        1. start workerpool
        2. find all queues
            1. queue names
                1. by default use "default"
                2. queue name bound to the role
                3. specify when starting the service
            2. queue name -> broker mapping
                1. by default use global broker
                2. specify when starting the service
            3. middlewares
                1. configuration
            4. encoder
        3. start dispatcher
        """
        self.worker_pool.thread_num = thread_num

        self._register_signal()

        self.consumer.start()
        self.worker.start()

    def stop(self):
        # Stop the worker and then the consumer. If not done in this order,
        # the worker may encounter an error during consumption
        self.worker.stop()
        self.consumer.stop()

        # Queues shouldn't be closed unless all pending messages are acked or
        # requeued
        for queue in self.queues:
            queue.close()

    def join(self):
        self.worker.join()
        self.consumer.join()

    def _register_signal(self):
        def handle_signal(signum, frame):
            self.stop()

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGINT, handle_signal)
