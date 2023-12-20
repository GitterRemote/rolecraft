import abc
import functools
from collections.abc import Callable
from typing import Any, Concatenate

from .broker import Broker
from .message import Message


# refer to:
# https://stackoverflow.com/questions/70329648/type-friendly-delegation-in-python
def copy_method_signature[**P, T](
    source: Callable[Concatenate[Any, str, P], T],
) -> Callable[[Callable[..., T]], Callable[Concatenate[Any, P], T]]:
    def wrapper(target: Callable[..., T]) -> Callable[Concatenate[Any, P], T]:
        @functools.wraps(source)
        def wrapped(self: Any, /, *args: P.args, **kwargs: P.kwargs) -> T:
            return target(self, *args, **kwargs)

        return wrapped

    return wrapper


def copy_msg_method_signature[**P, T](
    source: Callable[Concatenate[Any, Message, str | None, P], T],
) -> Callable[[Callable[..., T]], Callable[Concatenate[Any, Message, P], T]]:
    def wrapper(target: Callable[..., T]) -> Callable[Concatenate[Any, Message, P], T]:
        @functools.wraps(source)
        def wrapped(self: Any, /, *args: P.args, **kwargs: P.kwargs) -> T:
            return target(self, *args, **kwargs)

        return wrapped

    return wrapper


class Queue(abc.ABC):
    def __init__(self, name: str, broker: Broker) -> None:
        self.name = name
        self.broker = broker

    @copy_method_signature(Broker.enqueue)
    def enqueue(self, *args, **kwargs):
        return self.broker.enqueue(self.name, *args, **kwargs)

    @copy_method_signature(Broker.receive)
    def receive(self, *args, **kwargs):
        return self.broker.receive(self.name, *args, **kwargs)

    @copy_method_signature(Broker.qsize)
    def qsize(self, *args, **kwargs):
        return self.broker.qsize(self.name, *args, **kwargs)

    @copy_msg_method_signature(Broker.ack)
    def ack(self, *args, **kwargs):
        return self.broker.ack(*args, queue_name=self.name, **kwargs)

    @copy_msg_method_signature(Broker.nack)
    def nack(self, *args, **kwargs):
        return self.broker.nack(*args, queue_name=self.name, **kwargs)

    @copy_msg_method_signature(Broker.requeue)
    def requeue(self, *args, **kwargs):
        return self.broker.requeue(*args, queue_name=self.name, **kwargs)

    @copy_msg_method_signature(Broker.retry)
    def retry(self, *args, **kwargs):
        return self.broker.retry(*args, queue_name=self.name, **kwargs)

    def close(self):
        return self.broker.close()
