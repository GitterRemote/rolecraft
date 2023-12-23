import abc
from collections.abc import Sequence, Iterator
from rolecraft.queue import MessageQueue
from rolecraft.message import Message


class ConsumerStoppedError(Exception):
    pass


class Consumer(abc.ABC, Iterator):
    """A consumer that supports thread-safe consume method, which can be used
    by multiple worker together.

    If take it as a iterator, the __next__ method will raise StopIteration
    when Consumer.stop method is called.

    The main function of the consumer are the strategies to map the consume
    threads to the queues.
    """

    @abc.abstractmethod
    def __init__(self, queues: Sequence[MessageQueue]) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def consume(self, max_num=1) -> list[Message]:
        """The method is thread safe.

        Raises:
            ConsumerStoppedError: when call this method after stop() method is called. If the stop() is called during the process of this method, then it will return empty or partial result.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def __next__(self) -> Message:
        """It will always return a message until the consumer is stopped.

        The method is thread safe.

        raises: StopIteration when stopped
        """
        super().__next__
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
