from typing import Callable
import logging
import signal
from .queue import Queue
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
        queues: list[Queue] | None = None,
        config: Config | None = None,
        queue_configs: dict[str, Config] | None = None,
        broker_configs: dict[Broker, Config] | None = None,
        config_fetcher: ConfigFetcher | None = None,
    ) -> None:
        self.config_fetcher = config_fetcher or DefaultConfigFetcher(
            config, queue_configs, broker_configs
        )

        self.queues = _queue_builder.QueueBuilder(
            config_fetcher=self.config_fetcher,
            queue_names=queue_names,
            queues=queues,
        ).build()

        self.worker_pool = _worker_pool.WorkerPool()
        self.consumer = _consumer.Consumer(
            queues=self.queues,
            worker_pool=self.worker_pool,
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
        self._register_signal()

        self.worker_pool.thread_num = thread_num
        self.worker_pool.start()
        self.consumer.start()

        # allow worker_pool to work in the current thread
        self.worker_pool.join()
        self.consumer.join()

    def stop(self):
        # FIXME: order of the close
        for queue in self.queues:
            queue.close()
        self.consumer.stop()
        self.worker_pool.stop()

    def _register_signal(self):
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def _handle_signal(self, signum, frame):
        self.stop()
