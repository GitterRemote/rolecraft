from collections.abc import Sequence
import logging
import threading
from .queue import Queue
from .message import Message
from .config import ConfigFetcher

logger = logging.getLogger(__name__)


class ConsumerStoppedError(Exception):
    pass


class Consumer:
    """A consumer that supports thread-safe consume method, which can be used
    by multiple worker together.

    If take it as a iterator, the __next__ method will raise StopIteration
    when Consumer.stop method is called.

    The main function of the consumer are the strategies to map the consume
    threads to the queues.

    No-prefetch
    1. the combination of the round robin and block consuming
    2. no blocking + round robin 1 by 1
    3. no blocking + random (all consuming threads consume queues randomly)

    Prefetch
    1. dedicated consume threads + local queue (for cache)
    """

    def __init__(
        self,
        queues: Sequence[Queue],
        no_prefetch: bool,
        config_fetcher: ConfigFetcher | None,
        wait_time_seconds=10 * 60,
    ) -> None:
        self.queues = queues
        self.no_prefetch = no_prefetch
        self.config_fetcher = config_fetcher
        self.wait_time_seconds = wait_time_seconds

        self._stopped = False
        self._consumer_threads: list[threading.Thread] = []

    def consume(self, max_num=1) -> list[Message]:
        """The method is thread safe.

        raises: ConsumerStoppedError when stopped
        """
        if self._stopped:
            raise ConsumerStoppedError

        if self.no_prefetch:
            return self._fetch(max_num)

        if not self._consumer_threads:
            self._start_consumer_threads()

        return self._fetch_from_queue(max_num)

    def __next__(self) -> Message:
        """It will always return a message until the consumer is stopped.

        The method is thread safe.

        raises: StopIteration when stopped
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
        # TODO: cancel all receive futures

    def join(self):
        for thread in self._consumer_threads:
            thread.join()

    def _fetch(self, max_num: int) -> list[Message]:
        """should be thread-safe"""
        raise NotImplementedError

    def _fetch_from_queue(self, max_num: int) -> list[Message]:
        """should be thread-safe"""
        # TODO: wait for queue
        raise NotImplementedError

    def _start_consumer_threads(self):
        # FIXME: lock before start

        # start consumer threads to listen to queues from their brokers
        # create Message object then put them into worker pool
        for queue in self.queues:
            thread = threading.Thread(
                target=self._consume,
                args=(queue,),
                name=f"Consumer-{queue.name}",
            )
            self._consumer_threads.append(thread)
            thread.start()

    def _consume(self, queue: Queue):
        # TODO: move timeout to a configurable param
        while not self._stopped:
            msgs = queue.receive(wait_time_seconds=10 * 60)
            if not msgs:
                continue
            self._handle(msgs[0])

    def _handle(self, message: Message):
        if self._stopped:
            self._requeue(message)
            return

    def _requeue(self, message: Message):
        # TODO: add try catch
        logger.info("Requeue the message %s after stopping", message.id)
        if not message.requeue():
            logger.error("Requeue message error: %s", message.id)
