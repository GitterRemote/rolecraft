import functools
import typing
from collections.abc import Callable
from typing import Unpack

from rolecraft.broker import Broker
from rolecraft.queue import MessageQueue
from rolecraft.utils import typed_dict as _typed_dict

from . import queue_builder as _queue_builder
from .config_fetcher import ConfigFetcher
from .queue_builder import (
    QueueAndNameKeys,
    QueueBuildOptions,
    QueueConfigOptions,
)


class BatchBuildOptions(QueueAndNameKeys, QueueBuildOptions, total=False):
    ...


class QueueFactory:
    """It accepts user-defined queue related configurations, which are used during the queue building stage."""

    def __init__(self, config_fetcher: ConfigFetcher | None = None) -> None:
        self.config_fetcher = config_fetcher
        self._queues = dict[tuple[str, Broker | None], MessageQueue]()

    @typing.overload
    def get_or_build(
        self,
        *,
        raw_queue: MessageQueue | None = None,
    ) -> MessageQueue:
        ...

    @typing.overload
    def get_or_build(
        self,
        *,
        queue_name: str | None = None,
        **kwds: Unpack[QueueConfigOptions],
    ) -> MessageQueue:
        ...

    def get_or_build(
        self,
        *,
        queue_name: str | None = None,
        raw_queue: MessageQueue | None = None,
        **kwds: Unpack[QueueConfigOptions],
    ) -> MessageQueue:
        """Build the queue then cache it. The second call will fetch from the cache if with the same parameters."""
        if raw_queue:
            if queue_name is not None or kwds:
                raise ValueError(
                    "Extra parameters are not allowed with raw_queue"
                )
            key = (raw_queue.name, raw_queue.broker)
            builder = functools.partial(self._build_raw_queue, raw_queue)
        elif queue_name is None:
            raise ValueError("queue_name or raw_queue should be specified")
        else:
            key = (queue_name, kwds.get("broker"))
            builder = functools.partial(
                self._build_queue, queue_name=queue_name, **kwds
            )

        queue = self._cached_queue(key, builder)
        assert queue.name == queue_name
        return queue

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

    def _build_queue(
        self,
        queue_name: str,
        **kwds: Unpack[QueueConfigOptions],
    ) -> MessageQueue:
        # Be mind that the function can called more than once if another thread makes an additional call before the initial call has been completed and cached.
        builder = self._get_queue_builder()
        return builder.build_queue(queue_name, **kwds)

    def _build_raw_queue(self, raw_queue: MessageQueue) -> MessageQueue:
        # Be mind that the function can called more than once if another thread makes an additional call before the initial call has been completed and cached.
        builder = self._get_queue_builder()
        return builder.setup_queue(raw_queue)

    def clear(self):
        """Clear all cached queue. It is useful in UT"""
        self._queues.clear()

    def build_queues(
        self,
        *,
        config_fetcher: ConfigFetcher | None = None,
        **kwds: Unpack[BatchBuildOptions],
    ) -> list[MessageQueue]:
        """Always build brand new queues from the latest configuration."""
        build_options = _typed_dict.subset_dict(kwds, QueueBuildOptions)
        builder = self._get_queue_builder(config_fetcher, build_options)
        return builder.build(**kwds)

    def _get_queue_builder(
        self,
        config_fetcher: ConfigFetcher | None = None,
        build_options: QueueBuildOptions | None = None,
    ):
        config_fetcher = config_fetcher or self._get_config_fetcher()
        if not config_fetcher:
            raise ValueError("config_fetcher must exist")
        return _queue_builder.QueueBuilder(
            config_fetcher=config_fetcher, options=build_options or {}
        )

    def _get_config_fetcher(self) -> ConfigFetcher | None:
        return self.config_fetcher
