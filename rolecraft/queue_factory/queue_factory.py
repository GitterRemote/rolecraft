import typing
from typing import Unpack

from rolecraft import config as _config
from rolecraft.config import ConfigStore
from rolecraft.queue import MessageQueue

from . import queue_builder as _queue_builder
from .config_fetcher import ConfigFetcher
from .queue_builder import QueueConfigOptions, QueueAndNameKeys


class QueueFactory:
    """It accepts user-defined queue related configurations, which are used during the queue building stage."""

    def __init__(self, config_fetcher: ConfigFetcher | None = None) -> None:
        self.config_fetcher = config_fetcher
        self._queues: dict[str, MessageQueue] = {}

    @typing.overload
    def get_or_bulid(
        self,
        *,
        raw_queue: MessageQueue | None = None,
    ) -> MessageQueue:
        ...

    @typing.overload
    def get_or_bulid(
        self,
        *,
        queue_name: str | None = None,
        **kwds: Unpack[QueueConfigOptions],
    ) -> MessageQueue:
        ...

    def get_or_bulid(
        self,
        *,
        queue_name: str | None = None,
        raw_queue: MessageQueue | None = None,
        **kwds: Unpack[QueueConfigOptions],
    ) -> MessageQueue:
        """Always return the same Queue instance if it is the same name."""
        # Be mind that the function can called more than once if another thread makes an additional call before the initial call has been completed and cached.

        if raw_queue:
            if queue_name is not None or kwds:
                raise ValueError(
                    "Extra parameters are not allowed with raw_queue"
                )
            queue_name = raw_queue.name
        elif queue_name is None:
            raise ValueError("queue_name or raw_queue should be specified")

        if queue_name in self._queues:
            return self._queues[queue_name]

        # TODO: add thread lock

        builder = self._get_queue_builder()
        if raw_queue:
            queue = builder.wrap(raw_queue)
        else:
            queue = builder.build_one(queue_name, **kwds)

        assert queue
        self._queues[queue_name] = queue
        return queue

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
        return _queue_builder.QueueBuilder(config_fetcher=config_fetcher)

    def _get_config_fetcher(self) -> ConfigFetcher:
        return self.config_fetcher or self._get_config_store().fetcher

    def _get_config_store(self) -> ConfigStore:
        store = _config.DefaultConfigStore.get()
        assert store
        return store
