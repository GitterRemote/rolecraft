from collections.abc import Sequence
import logging
import threading
from .queue import Queue
from .message import Message
from .config import ConfigFetcher

logger = logging.getLogger(__name__)


class Consumer:
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

        # self._stop_event = threading.Event()
        self._stopped = False
        self._consumer_threads: list[threading.Thread] = []

    def receive(self, max_num=1) -> list[Message]:
        """The method is thread safe."""
        pass

    def __next__(self) -> Message | None:
        """The method is thread safe."""
        pass

    def __iter__(self):
        return self

    def start(self):
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

    def stop(self):
        self._stopped = True

    def join(self):
        for thread in self._consumer_threads:
            thread.join()

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
