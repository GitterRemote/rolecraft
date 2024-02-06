import abc
from typing import Self

from .queue import MessageQueue


class MiddlewareError(Exception):
    ...


@MessageQueue.register
class Middleware(abc.ABC):
    def __init__(self, queue: MessageQueue | None = None) -> None:
        self.queue: MessageQueue | None = queue

    def __getattr__(self, name: str):
        return getattr(self.queue, name)

    @abc.abstractmethod
    def __call__(self, queue: MessageQueue) -> MessageQueue:
        """Create a new middleware instance with the current options and the passed-in queue"""
        ...
