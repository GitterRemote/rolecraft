from typing import Any
import abc
from collections.abc import Callable
import concurrent.futures
import threading
from rolecraft import local as _local_mod


class WorkerPool(abc.ABC):
    @abc.abstractmethod
    def submit[**P](
        self, fn: Callable[P, Any], *args: P.args, **kwargs: P.kwargs
    ):
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def worker_num(self) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def start(self):
        raise NotImplementedError

    @abc.abstractmethod
    def stop(self):
        raise NotImplementedError

    @abc.abstractmethod
    def join(self):
        raise NotImplementedError


class ThreadWorkerPool(WorkerPool):
    def __init__(self, thread_num=1) -> None:
        self.thread_num = thread_num
        self._stopped = False
        self._executor: concurrent.futures.Executor | None = None
        self._futures: list[concurrent.futures.Future] = []
        self._stop_event = threading.Event()

    def submit[**P](
        self, fn: Callable[P, Any], *args: P.args, **kwargs: P.kwargs
    ):
        """This method is not thread-safe."""
        if self._stopped:
            raise RuntimeError(f"{self.__class__.__name__} has stopped!")

        if self.thread_num == 1:
            self._execute_fn(fn, args, kwargs)
            return

        assert self._executor

        if len(self._futures) >= self.thread_num:
            raise RuntimeError("Worker pool is full!")

        self._futures.append(
            self._executor.submit(self._execute_fn, fn, args, kwargs)
        )

    @property
    def worker_num(self) -> int:
        return self.thread_num

    def start(self):
        if self._stopped:
            raise RuntimeError(f"{self.__class__.__name__} has stopped!")

        # Executor must be initialized in he start method so that the thread
        # number can be updated after the class initialization and before
        # starting
        if self.thread_num == 1:
            return

        # TODO: replace ThreadPoolExecutor with using threading.Thread
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.thread_num,
            thread_name_prefix=self.__class__.__name__,
        )

    def stop(self):
        self._stopped = True

        # Notify the thread that it shoule be stopped
        self._stop_event.set()

        if self._executor:
            self._executor.shutdown(wait=False, cancel_futures=True)

    def join(self):
        concurrent.futures.wait(self._futures)

        for fs in self._futures:
            assert fs.done()

    def _execute_fn(self, fn: Callable, args, kwargs):
        _local_mod.local.stop_event = self._stop_event

        return fn(*args, **kwargs)
