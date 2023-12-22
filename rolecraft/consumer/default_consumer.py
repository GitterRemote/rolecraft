from collections.abc import Sequence
from rolecraft.queue import MessageQueue
from . import threaded_consumer as _threaded_consumer
from .consumer import Consumer


class NoPrefetch:
    pass


@Consumer.register
class DefaultConsumer:
    """A consumer that supports thread-safe consume method, which can be used
    by multiple worker together.

    If take it as a iterator, the __next__ method will raise StopIteration
    when Consumer.stop method is called.

    The main function of the consumer are the strategies to map the consume
    threads to the queues.
    """

    def __init__(
        self,
        queues: Sequence[MessageQueue],
        no_prefetch: NoPrefetch | None = None,
        prefetch_size=0,
    ) -> None:
        self.queues = queues
        self.no_prefetch = no_prefetch
        self.prefetch_size = prefetch_size

        if no_prefetch:
            raise NotImplementedError
        else:
            consumer = _threaded_consumer.ThreadedConsumer(
                queues=queues, prefetch_size=prefetch_size
            )

        self._consumer = consumer

    def __getattr__(self, name):
        getattr(self._consumer, name)
