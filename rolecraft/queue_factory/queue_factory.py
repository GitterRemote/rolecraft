import functools
import typing
from collections.abc import Hashable
from typing import Unpack

from rolecraft.queue import MessageQueue

from . import queue_builder as _queue_builder
from .config_fetcher import ConfigFetcher
from .queue_builder import QueueAndNameKeys, QueueConfigOptions


class QueueFactory:
    """It accepts user-defined queue related configurations, which are used during the queue building stage."""

    def __init__(self, config_fetcher: ConfigFetcher | None = None) -> None:
        self.config_fetcher = config_fetcher

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
        """Build the queue then cache it. The second call will fetch from the cache if with the same parameters.

        For simplification, current cache totally depends on functools.cache, where parameter changes, even the order, will affect the result.
        """
        if raw_queue:
            if queue_name is not None or kwds:
                raise ValueError(
                    "Extra parameters are not allowed with raw_queue"
                )
            return self._get_or_build_raw_queue(raw_queue)
        elif queue_name is None:
            raise ValueError("queue_name or raw_queue should be specified")

        if (middlewares := kwds.get("middlewares")) and not isinstance(
            middlewares, Hashable
        ):
            kwds["middlewares"] = tuple(middlewares)
        return self._get_or_build(queue_name=queue_name, **kwds)

    @functools.cache
    def _get_or_build(
        self,
        queue_name: str,
        **kwds: Unpack[QueueConfigOptions],
    ) -> MessageQueue:
        # Be mind that the function can called more than once if another thread makes an additional call before the initial call has been completed and cached.
        builder = self._get_queue_builder()
        return builder.build_one(queue_name, **kwds)

    @functools.cache
    def _get_or_build_raw_queue(self, raw_queue: MessageQueue) -> MessageQueue:
        # Be mind that the function can called more than once if another thread makes an additional call before the initial call has been completed and cached.
        builder = self._get_queue_builder()
        return builder.wrap(raw_queue)

    def build_queues(
        self,
        *,
        config_fetcher: ConfigFetcher | None = None,
        **kwds: Unpack[QueueAndNameKeys],
    ) -> list[MessageQueue]:
        """Always build brand new queues from the latest configuration."""
        builder = self._get_queue_builder(config_fetcher)
        return builder.build(**kwds)

    def _get_queue_builder(self, config_fetcher: ConfigFetcher | None = None):
        config_fetcher = config_fetcher or self._get_config_fetcher()
        if not config_fetcher:
            raise ValueError("config_fetcher must exist")
        return _queue_builder.QueueBuilder(config_fetcher=config_fetcher)

    def _get_config_fetcher(self) -> ConfigFetcher | None:
        return self.config_fetcher
