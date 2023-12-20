from typing import TypeVar
import queue
from .message import Message
from .worker_pool import WorkerPool
from .consumer import Consumer


class Worker:
    Q = TypeVar("Q", bound=queue.Queue | None)

    def __init__(self, worker_pool: WorkerPool, consumer: Consumer) -> None:
        self.worker_pool = worker_pool
        self.consumer = consumer

        self._stopped = False

    def start(self):
        # Assume worker pool is in the current process
        # TODO: otherwise, start the same number threads as the concurrency of the worker pool to consume.

        worker_pool = self.worker_pool
        for i in range(worker_pool.worker_num):
            worker_pool.submit(self._run, identity=i)

    def stop(self):
        self._stopped = True

    def _run(self, identity: int):
        """long running method"""
        # FIXME: check stopped or consumer will raise StopIteration
        for message in self.consumer:
            # TODO: should consumer always return a Message or return None when timeout?
            if not message:
                continue
            self._handle(message)

    def _handle(self, message: Message):
        # TODO: get role and its function, then call function
        pass

    def handle_error(
        self,
        message: Message,
        exception: Exception,
        result_queue: Q = None,
    ):
        try:
            if not self._do_ack:
                result_queue.put((message, exception))
                return

            if not message.nack(exception=exception):
                # TODO: log error
                pass
        except Exception as e:
            # TODO: log
            pass

    def handle_result(
        self,
        message: Message,
        result,
        result_queue: Q = None,
    ):
        try:
            if not self._do_ack:
                result_queue.put((message, result))
                return

            if not message.ack(result=result):
                # TODO: log error
                pass
        except Exception as e:
            # TODO: log
            pass
