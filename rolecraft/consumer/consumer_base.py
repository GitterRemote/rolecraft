import abc
from collections.abc import Sequence
import logging
from rolecraft.queue import MessageQueue
from rolecraft.message import Message
from .consumer import Consumer, ConsumerStoppedError

logger = logging.getLogger(__name__)


class ConsumerBase(Consumer):
    def __init__(self, queues: Sequence[MessageQueue]) -> None:
        self.queues = queues
        self._stopped = False

    def consume(self, max_num=1) -> list[Message]:
        """The method is thread safe.

        Raises:
            ConsumerStoppedError: when stopped
        """
        if self._stopped:
            raise ConsumerStoppedError
        return self._fetch_from_queues(max_num)

    def __next__(self) -> Message:
        """It will always return a message until the consumer is stopped.

        The method is thread safe.

        Raises:
            StopIteration: when stopped
        """
        while True:
            try:
                msgs = self.consume()
            except ConsumerStoppedError:
                raise StopIteration
            if msgs:
                return msgs[0]

    def __iter__(self):
        return self

    def start(self):
        pass

    def stop(self):
        self._stopped = True

    def join(self):
        pass

    @abc.abstractmethod
    def _fetch_from_queues(self, max_num: int) -> list[Message]:
        """should be thread-safe"""
        raise NotImplementedError

    def _requeue(self, *messages: Message):
        assert self._stopped
        for message in messages:
            logger.warning("Requeue the message %s after stopping", message.id)
            try:
                if not message.requeue():
                    logger.error("Requeue message error: %s", message.id)
            except Exception as e:
                logger.error(
                    "Requeue message error: %s", message.id, exc_info=e
                )
