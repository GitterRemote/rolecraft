import abc
from abc import abstractmethod
from typing import TypedDict


class EnqueueOptions(TypedDict, total=False):
    priority: int
    delay_millis: int
    create_queue: bool


class ReceiveFuture[Message](abc.ABC):
    @abstractmethod
    def result(self) -> list[Message]:
        """Return an empty list asap when cancel() is called."""
        raise NotImplementedError

    @abstractmethod
    def cancel(self):
        raise NotImplementedError

    @abstractmethod
    def __hash__(self) -> int:
        raise NotImplementedError


class Broker[Message](abc.ABC):
    @abstractmethod
    def enqueue(
        self,
        queue_name: str,
        message: Message,
        *,
        priority: int = 50,
        delay_millis: int = 0,
        create_queue: bool = False,
        **kwargs,
    ) -> str:
        raise NotImplementedError

    @abstractmethod
    def block_receive(
        self,
        queue_name: str,
        *,
        max_number: int = 1,
        wait_time_seconds: float | None = None,
        meta_keys: list[str] | None = None,
    ) -> ReceiveFuture[Message]:
        raise NotImplementedError

    def receive(
        self,
        queue_name: str,
        *,
        max_number: int = 1,
        meta_keys: list[str] | None = None,
    ) -> list[Message]:
        return self.block_receive(
            queue_name,
            wait_time_seconds=0,
            max_number=max_number,
            meta_keys=meta_keys,
        ).result()

    @abstractmethod
    def qsize(self, queue_name: str) -> int:
        """Returns the number of uncompleted tasks in th queue, including those
        that are in the process"""
        raise NotImplementedError

    @abstractmethod
    def ack(
        self,
        message: Message,
        queue_name: str | None = None,
        *,
        result=None,
    ) -> bool:
        """Marks the task as completed successfully

        Returns: if the message exists and the task is marked as completed,
            returns True. If the task is not working in process or it is failed
            permanently, the ack operation will fail to update it as completed.
        """
        raise NotImplementedError

    @abstractmethod
    def nack(
        self,
        message: Message,
        queue_name: str | None = None,
        *,
        exception: Exception,
    ) -> bool:
        """Marks the task as failed.

        Returns: if the task_id exists and the task is marked as failed,
            returns True. If the task is not working in process or it is
            compeleted successfully, the ack operation will fail to update
            it as failed.
        """
        raise NotImplementedError

    @abstractmethod
    def requeue(self, message: Message, queue_name: str | None = None) -> bool:
        """Requeue the task when give back the prefetched task.
        This will mark the task with the idle status.

        Returns:
        If the task is already ended or in retry, then the operation will fail.
        """
        raise NotImplementedError

    @abstractmethod
    def retry(
        self,
        message: Message,
        queue_name: str | None = None,
        *,
        delay_millis: int = 0,
        exception: Exception | None = None,
    ) -> bool:
        """Requeue retry-able task. This will mark the task with retry status.

        Returns:
        If the task is already eneded or in idle, then the operation will failed.
        """
        raise NotImplementedError

    def close(self):
        pass
