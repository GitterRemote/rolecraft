import logging
import threading

from rolecraft.thread_local import InterruptError

from .consumer import Consumer
from .message import Message
from .role_lib import ActionError, RoleHanger
from .worker_pool import ThreadWorkerPool, WorkerPool

logger = logging.getLogger(__name__)


class RoleMissingError(Exception):
    def __init__(self, message: Message, *args: object) -> None:
        self.message = message
        super().__init__(*args)


class Worker:
    def __init__(
        self,
        worker_pool: WorkerPool,
        consumer: Consumer,
        role_hanger: RoleHanger,
    ) -> None:
        self.worker_pool = worker_pool
        self.consumer = consumer
        self.role_hanger = role_hanger

        self._stopped = False

    def start(self):
        worker_pool = self.worker_pool
        if not isinstance(worker_pool, ThreadWorkerPool):
            # if the worker thread is not in the current process, it has no
            # access to the Queue objects, so it cannot finish the ack
            # operations.
            raise NotImplementedError

        worker_pool.start()

        for i in range(worker_pool.worker_num):
            worker_pool.submit(self._run, identity=i)

    def stop(self):
        self._stopped = True
        self.worker_pool.stop()

    def join(self):
        self.worker_pool.join()

    def _run(self, identity: int):
        """long running method"""
        # consumer will raise StopIteration when stopped
        for message in self.consumer:
            if self._stopped:
                self._handle_leftover(message)
                return
            self._handle(message)

        logger.info(f"Worker {threading.current_thread().name} stopped.")

    def _handle(self, message: Message):
        try:
            result = self._craft(message)
        except ActionError as e:
            if isinstance(e.__cause__, InterruptError):
                # when a long-running function is interrupted by the stop event
                self._handle_interrupt(message)
            else:
                self._handle_error(message, e)
        except Exception as e:
            self._handle_error(message, e)
        else:
            self._handle_result(message, result)

    def _craft(self, message: Message):
        role = self.role_hanger.pick(message.role_name)
        if not role:
            raise RoleMissingError(message=message)
        return role.craft(message)

    def _handle_leftover(self, message: Message):
        self._requeue(
            message,
            warning_log="Requeuing leftover message as the worker stopped: %s",
            error_log="Failed to requeue leftover message with ID: %s",
        )

    def _handle_interrupt(self, message: Message):
        self._requeue(
            message,
            warning_log="Requeuing messages due to worker interruption: %s",
            error_log="Failed to requeue message with ID: %s",
        )

    def _requeue(self, message: Message, warning_log: str, error_log: str):
        logger.warning(warning_log, message.id)
        try:
            message.requeue()
        except Exception as e:
            logger.error(error_log, message.id, exc_info=e)

    def _handle_error(
        self,
        message: Message,
        exception: Exception,
    ):
        logger.error(
            "Error handling message with ID: %s",
            message.id,
            exc_info=exception,
        )
        try:
            message.nack(exception=exception)
        except Exception as e:
            logger.error(
                "Failed to nack message with ID: %s",
                message.id,
                exc_info=e,
            )

    def _handle_result(
        self,
        message: Message,
        result,
    ):
        try:
            message.ack(result=result)
        except Exception as e:
            logger.error(
                "Failed to ack message with ID: %s",
                message.id,
                exc_info=e,
            )
