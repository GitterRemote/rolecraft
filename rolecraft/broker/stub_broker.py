import collections
import dataclasses
import threading
import uuid
from collections import deque

from . import error as _error
from .base_broker import BaseBroker
from .raw_message import HeaderBytesRawMessage
from .receive_future import ReceiveFuture


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

    def enqueue(self, msg: HeaderBytesRawMessage, **options) -> str:
        if not msg.id:
            msg.id = uuid.uuid4().hex
        self._msg_queue.append(msg)

        # The following code can be in an async job
        with self._lock:
            if self._waiting_queue:
                self._waiting_queue[0].event.set()

        return msg.id

    def _receive_directly(self, num: int) -> list[HeaderBytesRawMessage]:
        msgs = list[HeaderBytesRawMessage]()
        while len(msgs) < num and self._msg_queue:
            msg = self._msg_queue.popleft()
            self._processing_msgs[msg.id] = msg
            msgs.append(msg)
        return msgs

    def ack(self, message: HeaderBytesRawMessage):
        try:
            self._processing_msgs.pop(message.id)
        except KeyError:
            raise _error.MessageNotFound

    def nack(self, message: HeaderBytesRawMessage):
        return self.ack(message)

    def requeue(self, message: HeaderBytesRawMessage):
        try:
            msg = self._processing_msgs.pop(message.id)
        except KeyError:
            raise _error.MessageNotFound
        else:
            self.enqueue(msg)

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
                    self._waiting_queue[0] is proxy
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
class _ReceiveFuture(ReceiveFuture[list[HeaderBytesRawMessage]]):
    _proxy: _QueueWaitProxy

    def result(self) -> list[HeaderBytesRawMessage]:
        return self._proxy.result()

    def cancel(self):
        self._proxy.cancel()

    def __hash__(self) -> int:
        return id(self._proxy)


class StubBroker(BaseBroker):
    def __init__(self) -> None:
        self._queues = collections.defaultdict[str, _Queue](_Queue)

    def enqueue(
        self,
        queue_name: str,
        message: HeaderBytesRawMessage,
        **options,
    ) -> str:
        delay_millis = options.get("delay_millis") or 0
        if "priority" in options or delay_millis > 0:
            raise NotImplementedError
        queue = self._queues[queue_name]
        return queue.enqueue(message, **options)

    def block_receive(
        self,
        queue_name: str,
        *,
        max_number: int = 1,
        wait_time_seconds: float | None = None,
        header_keys: list[str] | None = None,
    ) -> ReceiveFuture[list[HeaderBytesRawMessage]]:
        queue = self._queues[queue_name]
        proxy = queue.receive(max_number, wait_time_seconds)
        return _ReceiveFuture(proxy)

    def qsize(self, queue_name: str) -> int:
        return len(self._queues[queue_name])

    def ack(
        self,
        message: HeaderBytesRawMessage,
        queue_name: str,
        *,
        result=None,
    ):
        return self._queues[queue_name].ack(message)

    def nack(
        self,
        message: HeaderBytesRawMessage,
        queue_name: str,
        *,
        exception: Exception,
    ):
        return self._queues[queue_name].nack(message)

    def requeue(self, message: HeaderBytesRawMessage, queue_name: str):
        return self._queues[queue_name].requeue(message)

    def prepare_queue(self, queue_name: str, **kwds):
        if queue_name not in self._queues:
            self._queues[queue_name] = _Queue()
