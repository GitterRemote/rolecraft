from collections.abc import Sequence
import contextlib
import logging
import threading
from rolecraft.queue import MessageQueue
from rolecraft.message import Message
from rolecraft import notify_queue as _notify_queue
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
        self._result_future_sets = set()

    def stop(self):
        super().stop()

        # Cancel blocking receiving to stop consumer threads
        for future in self._result_future_sets:
            future.cancel()

        # Requeue the messages in the local queue
        if not self.no_prefetch:
            # Unblock worker threads waiting for the local quue
            self._local_queue.notify_all()

    def join(self):
        super().join()

        for thread in self._consumer_threads:
            thread.join()

        # handle leftover messages
        self._requeue(*self._local_queue)

    def _fetch_from_queues(self, max_num: int) -> list[Message]:
        if not self._consumer_threads:
            self._start_consumer_threads()
        return self._fetch_from_local_queue()

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

        if self._stopped:
            # handle leftover messages. This can be handled in the Consumer's
            # stop or join method because they may end before the end of worker
            # thread.
            self._requeue(*msgs)
            return []

        return msgs

    def _start_consumer_threads(self):
        """It should be thread-safe"""
        with self._lock:
            if self._consumer_threads:
                return

            for queue in self.queues:
                thread = threading.Thread(
                    target=self._consume,
                    args=(queue),
                    name=f"{self.__class__.__name__}-{queue.name}",
                )
                self._consumer_threads.append(thread)
                thread.start()

    def _consume(self, queue: MessageQueue):
        local_queue = self._local_queue
        consumer_num = len(self.queues)
        assert local_queue.maxsize > 0
        batch_size = int(local_queue.maxsize / consumer_num) + 1

        while not self._stopped:
            future = queue.block_receive(max_number=batch_size)
            with self._hook_stop_event(future) as hooked:
                if not hooked:
                    future.cancel()
                for msg in future.result():
                    local_queue.put(msg)

    @contextlib.contextmanager
    def _hook_stop_event(self, future):
        """hook the fs.cancel() with consumer's stop event."""
        if self._stopped:
            yield False
            return

        self._result_future_sets.add(future)
        yield True
        self._result_future_sets.remove(future)
