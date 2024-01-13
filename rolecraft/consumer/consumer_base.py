import abc
import logging
from collections.abc import Sequence

from rolecraft.message import Message
from rolecraft.queue import MessageQueue

from .consumer import Consumer, ConsumerStoppedError

logger = logging.getLogger(__name__)


class ConsumerBase(Consumer):
    def __init__(self, queues: Sequence[MessageQueue]) -> None:
        self.queues = queues
        self._stopped = False

    def consume(self, max_num=1) -> list[Message]:
        if self._stopped:
            raise ConsumerStoppedError
        msgs = self._fetch_from_queues(max_num)
        if self._stopped:
            # handle leftover messages. This can not be handled in the
            # Consumer's stop or join methods because they may end before the
            # end of worker thread.
            self._requeue(*msgs)
            return []
        return msgs

    def __next__(self) -> Message:
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
        """It should be made thread-safe. It is necessary to check the stopped flag if it is expected to run for a prolonged period and return a partial result. The parant method consume() will requeue them if necessary."""
        raise NotImplementedError

    def _requeue(self, *messages: Message):
        assert self._stopped
        for message in messages:
            logger.warning("Requeue the message %s after stopping", message.id)
            try:
                message.requeue()
            except Exception as e:
                logger.error(
                    "Requeue message error: %s", message.id, exc_info=e
                )
