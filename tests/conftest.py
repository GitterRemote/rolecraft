import collections
import dataclasses
import threading
from collections import deque

from rolecraft.broker import Broker, HeaderBytesRawMessage, ReceiveFuture


@dataclasses.dataclass
class _QueueWaitProxy:
    num: int
    wait_time_seconds: float | None
    queue: "_Queue"
    event: threading.Event = dataclasses.field(default_factory=threading.Event)
    cancelled: bool = False

    def cancel(self):
        self.cancelled = True
        self.event.set()

    def result(self) -> list[HeaderBytesRawMessage]:
        return self.queue.receive_with_proxy(self)


@dataclasses.dataclass
class _Queue:
    _msg_queue: deque[HeaderBytesRawMessage] = dataclasses.field(
        default_factory=deque
    )
    _processing_msgs: dict[str, HeaderBytesRawMessage] = dataclasses.field(
        default_factory=dict
    )

    _lock: threading.RLock = dataclasses.field(default_factory=threading.RLock)
    _waiting_queue: deque[_QueueWaitProxy] = dataclasses.field(
        default_factory=deque
    )

    def __len__(self) -> int:
        return len(self._msg_queue) + len(self._processing_msgs)

    def enqueue(self, msg: HeaderBytesRawMessage, **options) -> bool:
        self._msg_queue.append(msg)

        # The following code can be in an async job
        with self._lock:
            if self._waiting_queue:
                self._waiting_queue[0].event.set()

        return True

    def _receive_directly(self, num: int) -> list[HeaderBytesRawMessage]:
        msgs = list[HeaderBytesRawMessage]()
        while len(msgs) < num and self._msg_queue:
            msg = self._msg_queue.popleft()
            self._processing_msgs[msg.id] = msg
            msgs.append(msg)
        return msgs

    def ack(self, message: HeaderBytesRawMessage) -> bool:
        try:
            self._processing_msgs.pop(message.id)
            return True
        except KeyError:
            return False

    def nack(self, message: HeaderBytesRawMessage) -> bool:
        return self.ack(message)

    def requeue(self, message: HeaderBytesRawMessage) -> bool:
        try:
            msg = self._processing_msgs.pop(message.id)
        except KeyError:
            return False
        else:
            self.enqueue(msg)
            return True

    def receive(
        self, num: int, wait_time_seconds: float | None
    ) -> _QueueWaitProxy:
        return _QueueWaitProxy(num, wait_time_seconds, self)

    def receive_with_proxy(
        self, proxy: _QueueWaitProxy
    ) -> list[HeaderBytesRawMessage]:
        # TODO: improvement: receive directly before get the lock

        with self._lock:  # assume it will acquire the lock soon
            if not self._waiting_queue:
                if msgs := self._receive_directly(proxy.num):
                    return msgs

            self._waiting_queue.append(proxy)

        notified = proxy.event.wait(proxy.wait_time_seconds)

        with self._lock:
            if not notified or proxy.cancelled:
                # Edge case: self is the top one and being notifed and timed-out near the same time.
                if (
                    self._waiting_queue[0] is self
                    and len(self._waiting_queue) > 1
                    and self._msg_queue
                ):
                    self._waiting_queue[1].event.set()

                self._waiting_queue.remove(proxy)
                return []

            assert self._waiting_queue[0] is proxy
            self._waiting_queue.popleft()

            msgs = self._receive_directly(proxy.num)
            assert msgs

            if self._waiting_queue:
                self._waiting_queue[0].event.set()

            return msgs


@dataclasses.dataclass
class _ReceiveFuture(ReceiveFuture[HeaderBytesRawMessage]):
    queue: _Queue
    wait_time_seconds: float | None = None
    num: int = 1
    proxy: _QueueWaitProxy | None = None

    def result(self) -> list[HeaderBytesRawMessage]:
        proxy = self.queue.receive(self.num, self.wait_time_seconds)
        self.proxy = proxy
        return proxy.result()

    def cancel(self):
        if not self.proxy:
            raise RuntimeError("The receive haven't started")
        return self.proxy.cancel()


class StubBroker(Broker[HeaderBytesRawMessage]):
    def __init__(self) -> None:
        self._queues = collections.defaultdict[str, _Queue](_Queue)

    def enqueue(
        self,
        queue_name: str,
        message: HeaderBytesRawMessage,
        **options,
    ) -> str:
        if "priority" in options or "delay_millis" in options:
            raise NotImplementedError
        queue = self._queues[queue_name]
        queue.enqueue(message, **options)
        return message.id

    def block_receive(
        self,
        queue_name: str,
        *,
        max_number: int = 1,
        wait_time_seconds: float | None = None,
        meta_keys: list[str] | None = None,
    ) -> ReceiveFuture[HeaderBytesRawMessage]:
        queue = self._queues[queue_name]
        return _ReceiveFuture(queue, wait_time_seconds, max_number)

    def qsize(self, queue_name: str) -> int:
        return len(self._queues[queue_name])

    def ack(
        self,
        message: HeaderBytesRawMessage,
        queue_name: str,
        *,
        result=None,
    ) -> bool:
        return self._queues[queue_name].ack(message)

    def nack(
        self,
        message: HeaderBytesRawMessage,
        queue_name: str,
        *,
        exception: Exception,
    ) -> bool:
        return self._queues[queue_name].nack(message)

    def requeue(self, message: HeaderBytesRawMessage, queue_name: str) -> bool:
        return self._queues[queue_name].requeue(message)

    def retry(
        self,
        message: HeaderBytesRawMessage,
        queue_name: str,
        *,
        delay_millis: int = 0,
        exception: Exception | None = None,
    ) -> bool:
        queue = self._queues[queue_name]
        if queue.ack(message):
            # TODO: copy a message
            retries = int(message.headers.get("retries") or 0)
            message.headers["retries"] = retries + 1
            return queue.enqueue(message, delay_millis=delay_millis)
        return False
