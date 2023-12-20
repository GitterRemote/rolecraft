import abc
from abc import abstractmethod
from rolecraft.message import Message


class Broker(abc.ABC):
    @abstractmethod
    def enqueue(
        self,
        queue_name: str,
        data: bytes,
        *,
        priority: int = 50,
        delay_millis: int = 0,
        **kwargs,
    ) -> str:
        pass

    @abstractmethod
    def receive(
        self, queue_name: str, max_number: int = 1, wait_time_seconds: int = 0
    ) -> list[Message]:
        """Receives a list of tasks from the Queue, and the queue will put the tasks on the in process status, which will not be received by other workers before the visibility timeout.

        The block should be interrupted when close method is called.
        """
        pass

    @abstractmethod
    def qsize(self, queue_name: str) -> int:
        """Returns the number of uncompleted tasks in th queue, including those are in the process"""
        pass

    @abstractmethod
    def ack(
        self,
        message: Message,
        *,
        queue_name: str | None = None,
        result=None,
    ) -> bool:
        """Marks the task as completed successfully

        Returns: if the task_id exists and the task is marked as completed, returns True. If the task is not working in process or it is failed permanently, the ack operation will fail to update it as completed.
        """
        pass

    @abstractmethod
    def nack(
        self,
        message: Message,
        *,
        queue_name: str | None = None,
        exception: Exception,
    ) -> bool:
        """Marks the task as failed.

        Returns: if the task_id exists and the task is marked as failed, returns True. If the task is not working in process or it is compeleted successfully, the ack operation will fail to update it as failed.
        """
        pass

    @abstractmethod
    def requeue(self, message: Message, *, queue_name: str | None = None) -> bool:
        """Requeue the task when give back the prefetched task. This will mark the task with the idle status.

        Returns:
        If the task is already ended or in retry, then the operation will fail.
        """
        pass

    @abstractmethod
    def retry(
        self,
        message: Message,
        *,
        queue_name: str | None = None,
        delay_millis: int = 0,
        exception: Exception | None = None,
    ) -> bool:
        """Requeue retry-able task. This will mark the task with retry status.

        Returns:
        If the task is already eneded or in idle, then the operation will failed.
        """
        pass

    def close(self):
        pass
