from typing import Self
from rolecraft.queue import MessageQueue


@MessageQueue.register
class Middleware:
    def __init__(self) -> None:
        self.queue: MessageQueue | None = None

    def __getattr__(self, name):
        return getattr(self.queue, name)

    def __call__(self, queue: MessageQueue) -> Self:
        self.queue = queue
        return self
