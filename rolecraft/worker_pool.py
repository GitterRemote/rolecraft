from collections.abc import Callable
from .message import Message


class WorkerPool:
    def __init__(self, thread_num=1) -> None:
        self.thread_num = thread_num

    def submit[**P](self, fn: Callable[P], *args: P.args, **kwargs: P.kwargs):
        """This is a block submit"""
        pass

    @property
    def worker_num(self) -> int:
        return 0

    def start(self):
        pass

    def stop(self):
        pass

    def join(self):
        pass
