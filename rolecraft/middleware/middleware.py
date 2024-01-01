from typing import Self
from rolecraft.queue import MessageQueue


@MessageQueue.register
class Middleware:
    def __init__(self, queue: MessageQueue | None = None) -> None:
        self.queue: MessageQueue | None = queue

    def __getattr__(self, name):
        return getattr(self.queue, name)

    def __call__(self, queue: MessageQueue) -> Self:
        return self.copy_with(queue)

    def copy_with(self, queue: MessageQueue) -> Self:
        return self.__class__(queue=queue, **self.options)
