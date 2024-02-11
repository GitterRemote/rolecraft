import logging
import signal

from rolecraft.queue import MessageQueue

from .consumer import Consumer
from .worker import Worker
from .worker_pool import ThreadWorkerPool, WorkerPool

logger = logging.getLogger(__name__)


class Service:
    def __init__(
        self,
        *,
        queues: list[MessageQueue],
        consumer: Consumer,
        worker: Worker,
        worker_pool: WorkerPool,
    ) -> None:
        self.queues = queues
        self.consumer = consumer
        self.worker = worker
        self.worker_pool = worker_pool

    def start(
        self,
        *,
        thread_num: int | None = None,
        ignore_signal: bool = False,
    ):
        if isinstance(self.worker_pool, ThreadWorkerPool):
            self.worker_pool.thread_num = thread_num or 1
        elif thread_num:
            raise NotImplementedError("Unsupported worker pool")

        if not ignore_signal:
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
            logger.info(f"Signal {signal.Signals(signum).name} received.")
            self.stop()

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)
