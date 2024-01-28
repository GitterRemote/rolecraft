import contextlib
import logging
import math
import threading
import time
from collections.abc import Sequence

from rolecraft.message import Message
from rolecraft.queue import MessageQueue

from . import notify_queue as _notify_queue
from .consumer_base import ConsumerBase

logger = logging.getLogger(__name__)


class ThreadedConsumer(ConsumerBase):
    def __init__(
        self,
        queues: Sequence[MessageQueue],
        prefetch_size: int,
    ) -> None:
        if prefetch_size < 1:
            raise ValueError("prefetch size should be greater than 0")

        super().__init__(queues=queues)
        self.prefetch_size = prefetch_size

        self._consumer_threads: list[threading.Thread] = []
        self._lock = threading.Lock()
        self._local_queue = _notify_queue.NotifyQueue[Message](
            maxsize=self.prefetch_size
        )
        self._result_futures_set = set()

    def stop(self):
        super().stop()

        # Cancel blocking receiving to stop consumer threads
        for future in self._result_futures_set:
            logger.debug("Cancel result future %r", future)
            future.cancel()

        # Unblock worker threads waiting for the local quue
        logger.debug("Notify all worker threads listening to the local queue")
        self._local_queue.notify_all()

    def join(self):
        super().join()

        # Handle leftover messages
        # Using a requeuing thread to fetch messages from the local queue to
        # unblock consumer threads
        logger.debug("Requeue messages from the local queue")
        t, ev = self._requeue_local_queue_messages()

        # Join consumer threads
        # it is possible that a consumer thread is blocked by the put method.
        for thread in self._consumer_threads:
            thread.join()

        ev.set()
        t.join()
        logger.debug("Consumer stopped")

    def _requeue_local_queue_messages(
        self,
    ) -> tuple[threading.Thread, threading.Event]:
        consumer_stopped = threading.Event()

        def requeue():
            while not consumer_stopped.is_set():
                msg = self._local_queue.get_nowait()
                if msg:
                    self._requeue(msg)
                else:
                    time.sleep(0.1)
            self._requeue(*self._local_queue)

        t = threading.Thread(
            target=requeue, name=f"{self.__class__.__name__}-Requeue"
        )
        t.start()
        return t, consumer_stopped

    def _fetch_from_queues(self, max_num: int) -> list[Message]:
        if not self._consumer_threads:
            self._start_consumer_threads()
        return self._fetch_from_local_queue(max_num)

    def _fetch_from_local_queue(self, max_num: int) -> list[Message]:
        """should be thread-safe"""
        msg = self._local_queue.get(wakeup_until_notify_all=True)
        if not msg:
            assert self._stopped
            return []

        msgs = [msg]

        while len(msgs) < max_num:
            msg = self._local_queue.get_nowait()
            if not msg:
                break
            msgs.append(msg)

        return msgs

    def _start_consumer_threads(self):
        """It should be thread-safe"""
        with self._lock:
            if self._consumer_threads:
                return

            for queue in self.queues:
                thread = threading.Thread(
                    target=self._consume,
                    args=(queue,),
                    name=f"{self.__class__.__name__}_queue_{queue.name}",
                )
                self._consumer_threads.append(thread)
                thread.start()

    def _consume(self, queue: MessageQueue):
        local_queue = self._local_queue
        consumer_num = len(self.queues)
        assert local_queue.maxsize > 0
        batch_size = math.ceil(local_queue.maxsize / consumer_num)

        while not self._stopped:
            future = queue.block_receive(max_number=batch_size)
            with self._hook_stop_event(future) as hooked:
                if not hooked:
                    future.cancel()
                for msg in future.result():
                    local_queue.put(msg)

        logger.info(f"Consumer {threading.current_thread().name} stopped.")

    @contextlib.contextmanager
    def _hook_stop_event(self, future):
        """hook the fs.cancel() with consumer's stop event."""
        self._result_futures_set.add(future)
        yield not self._stopped
        self._result_futures_set.remove(future)
