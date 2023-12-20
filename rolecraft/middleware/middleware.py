from typing import Self
from rolecraft.queue import Queue


@Queue.register
class Middleware:
    def __init__(self) -> None:
        self.queue: Queue | None = None

    def __getattr__(self, name):
        return getattr(self.queue)

    def __call__(self, queue: Queue) -> Self:
        self.queue = queue
        return self
