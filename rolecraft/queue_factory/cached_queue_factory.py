import functools
from collections.abc import Callable
from typing import Unpack

from rolecraft.broker import Broker
from rolecraft.queue import MessageQueue

from .config_fetcher import ConfigFetcher
from .queue_builder import QueueConfigOptions
from .queue_factory import QueueFactory


class CachedQueueFactory(QueueFactory):
    def __init__(self, config_fetcher: ConfigFetcher | None = None) -> None:
        super().__init__(config_fetcher)
        self._queues = dict[tuple[str, Broker | None], MessageQueue]()

    def clear(self):
        """Clear all cached queue. It is useful in UT"""
        self._queues.clear()

    def _build_queue(
        self, queue_name: str, **kwds: Unpack[QueueConfigOptions]
    ) -> MessageQueue:
        # Be mind that the function can called more than once if another thread makes an additional call before the initial call has been completed and cached.
        key = (queue_name, kwds.get("broker"))
        builder = functools.partial(
            super()._build_queue, queue_name=queue_name, **kwds
        )
        return self._cached_queue(key, builder)

    def _build_raw_queue(self, raw_queue: MessageQueue) -> MessageQueue:
        # Be mind that the function can called more than once if another thread makes an additional call before the initial call has been completed and cached.
        key = (raw_queue.name, raw_queue.broker)
        builder = functools.partial(super()._build_raw_queue, raw_queue)
        return self._cached_queue(key, builder)

    def _cached_queue(
        self,
        key: tuple[str, Broker | None],
        builder: Callable[[], MessageQueue],
    ) -> MessageQueue:
        if queue := self._queues.get(key):
            return queue

        queue = builder()
        self._queues[key] = queue
        return queue
