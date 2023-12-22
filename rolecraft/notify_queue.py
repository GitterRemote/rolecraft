from collections.abc import Iterator
import queue
import threading
from typing import Iterator


class NotifyQueue[Item](Iterator):
    def __init__(self, maxsize: int = 0) -> None:
        self._queue = queue.Queue[Item](maxsize=maxsize)
        self._condition = threading.Condition()
        self._all_notfied = False

    @property
    def maxsize(self):
        return self._queue.maxsize

    def get_nowait(self) -> Item | None:
        try:
            return self._queue.get_nowait()
        except queue.Empty:
            return None

    def get(self, wakeup_until_notify_all=False) -> Item | None:
        """Blocking get an item from the queue"""
        item = self.get_nowait()
        if item is not None:
            return item

        with self._condition:
            # avoid dead-lock: wait only when queue is empty
            if not self._queue.empty():
                item = self.get_nowait()
                if item is not None:
                    return item

            while True:
                self._condition.wait()
                item = self.get_nowait()
                if item is not None:
                    return item

                if not wakeup_until_notify_all or self._all_notfied:
                    return

    def put(self, item: Item):
        """Blocking put"""
        self._queue.put(item)

        with self._condition:
            self._condition.notify()

    def notify_all(self):
        with self._condition:
            self._all_notfied = True
            self._condition.notify_all()

    # Iterator method
    def __next__(self) -> Item:
        try:
            return self._queue.get_nowait()
        except queue.Empty:
            raise StopIteration
