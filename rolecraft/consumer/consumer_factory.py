from collections.abc import Sequence
from typing import Protocol
from rolecraft.queue import MessageQueue
from . import threaded_consumer as _threaded_consumer
from .consumer import Consumer


class NoPrefetch:
    pass


class ConsumerFactory(Protocol):
    def __call__(
        self,
        queues: Sequence[MessageQueue],
        no_prefetch: NoPrefetch | None = None,
        prefetch_size=0,
    ) -> Consumer:
        ...


class DefaultConsumerFactory(ConsumerFactory):
    def create(
        self,
        queues: Sequence[MessageQueue],
        no_prefetch: NoPrefetch | None = None,
        prefetch_size=0,
    ) -> Consumer:
        if no_prefetch:
            raise NotImplementedError
        return _threaded_consumer.ThreadedConsumer(
            queues=queues, prefetch_size=prefetch_size
        )

    __call__ = create
