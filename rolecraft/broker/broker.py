import abc
from abc import abstractmethod
from typing import TypedDict

from .receive_future import ReceiveFuture


class EnqueueOptions(TypedDict, total=False):
    priority: int
    delay_millis: int
    auto_create_queue: bool


class Broker[Message](abc.ABC):
    @abstractmethod
    def enqueue(
        self,
        queue_name: str,
        message: Message,
        *,
        priority: int = 50,
        delay_millis: int = 0,
        auto_create_queue: bool = False,
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
        header_keys: list[str] | None = None,
        **kwargs,
    ) -> ReceiveFuture[list[Message]]:
        raise NotImplementedError

    def receive(
        self,
        queue_name: str,
        *,
        max_number: int = 1,
        header_keys: list[str] | None = None,
        **kwargs,
    ) -> list[Message]:
        return self.block_receive(
            queue_name,
            wait_time_seconds=0,
            max_number=max_number,
            header_keys=header_keys,
            **kwargs,
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
    ):
        """Marks the message as completed successfully

        If the operation fails, raises errors.
        """
        raise NotImplementedError

    @abstractmethod
    def nack(
        self,
        message: Message,
        queue_name: str | None = None,
        *,
        exception: Exception,
    ):
        """Marks the message as permanently failed.

        If the operation fails, raises errors.
        """
        raise NotImplementedError

    @abstractmethod
    def requeue(self, message: Message, queue_name: str | None = None):
        """Requeue the message.

        If the operation fails, raises errors.
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
    ) -> Message:
        """Retry the message, as the handling of it has failed.

        Returns: returns a message object.
        """
        raise NotImplementedError

    def close(self):
        pass

    def prepare_queue(self, queue_name: str, **kwargs):
        """create the queue if not exists and prepare relevant resources if necessary"""
        pass
