import abc
import functools
from collections.abc import Callable
from typing import Any, Concatenate

from .broker import Broker
from .message import Message
from .encoder import Encoder


# refer to:
# https://stackoverflow.com/questions/70329648/type-friendly-delegation-in-python
def copy_method_signature[CLS, **P, T](
    source: Callable[Concatenate[Any, str, P], T],
) -> Callable[[Callable[..., T]], Callable[Concatenate[CLS, P], T]]:
    def wrapper(target: Callable[..., T]) -> Callable[Concatenate[CLS, P], T]:
        @functools.wraps(source)
        def wrapped(self: CLS, /, *args: P.args, **kwargs: P.kwargs) -> T:
            return target(self, *args, **kwargs)

        return wrapped

    return wrapper


# Omit `queue_name: str | None` from the Broker's method signature
def copy_msg_method_signature[CLS, **P, T](
    source: Callable[Concatenate[Any, Message, str | None, P], T],
) -> Callable[[Callable[..., T]], Callable[Concatenate[CLS, Message, P], T]]:
    def wrapper(
        target: Callable[..., T],
    ) -> Callable[Concatenate[CLS, Message, P], T]:
        @functools.wraps(source)
        def wrapped(self: CLS, /, *args: P.args, **kwargs: P.kwargs) -> T:
            return target(self, *args, **kwargs)

        return wrapped

    return wrapper


class MessageQueue[RawMessage](abc.ABC):
    def __init__(
        self,
        name: str,
        broker: Broker[RawMessage],
        encoder: Encoder[RawMessage],
        wait_time_seconds: int | None = None,
    ) -> None:
        self.name = name
        self.broker = broker
        self.encoder = encoder
        self.wait_time_seconds = wait_time_seconds

    @copy_method_signature(Broker[Message].enqueue)
    def enqueue(self, *args, **kwargs):
        return self.broker.enqueue(self.name, *args, **kwargs)

    @copy_method_signature(Broker[Message].block_receive)
    def block_receive(self, *args, **kwargs):
        """If the wait_time_seconds is None, it will be default value of the
        queue."""
        kwargs.setdefault("wait_time_seconds", self.wait_time_seconds)
        future = self.broker.block_receive(self.name, *args, **kwargs)
        future.result = self._wrap_result(future.result)
        return future

    def _wrap_result(self, fn: Callable[..., list[RawMessage]]):
        return lambda: [self.encoder.decode(m) for m in fn()]

    @copy_method_signature(Broker[Message].receive)
    def receive(self, *args, **kwargs):
        msgs = self.broker.receive(self.name, *args, **kwargs)
        return [self.encoder.decode(m) for m in msgs]

    @copy_method_signature(Broker[Message].qsize)
    def qsize(self, *args, **kwargs):
        return self.broker.qsize(self.name, *args, **kwargs)

    @copy_msg_method_signature(Broker[Message].ack)
    def ack(self, message: Message, *args, **kwargs):
        return self.broker.ack(
            self.encoder.encode(message), *args, queue_name=self.name, **kwargs
        )

    @copy_msg_method_signature(Broker[Message].nack)
    def nack(self, message: Message, *args, **kwargs):
        return self.broker.nack(
            self.encoder.encode(message), *args, queue_name=self.name, **kwargs
        )

    @copy_msg_method_signature(Broker[Message].requeue)
    def requeue(self, message: Message, *args, **kwargs):
        return self.broker.requeue(
            self.encoder.encode(message), *args, queue_name=self.name, **kwargs
        )

    @copy_msg_method_signature(Broker[Message].retry)
    def retry(self, message: Message, *args, **kwargs):
        return self.broker.retry(
            self.encoder.encode(message), *args, queue_name=self.name, **kwargs
        )

    def close(self):
        return self.broker.close()
