import typing
from typing import Unpack

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

    @typing.overload
    def build_queue(
        self, *, raw_queue: MessageQueue | None = None
    ) -> MessageQueue:
        ...

    @typing.overload
    def build_queue(
        self,
        *,
        queue_name: str | None = None,
        **kwds: Unpack[QueueConfigOptions],
    ) -> MessageQueue:
        ...

    def build_queue(
        self,
        *,
        queue_name: str | None = None,
        raw_queue: MessageQueue | None = None,
        **kwds: Unpack[QueueConfigOptions],
    ) -> MessageQueue:
        if raw_queue:
            if queue_name is not None or kwds:
                raise ValueError(
                    "Extra parameters are not allowed with raw_queue"
                )
            return self._build_raw_queue(raw_queue)
        elif queue_name is None:
            raise ValueError("queue_name or raw_queue should be specified")
        else:
            queue = self._build_queue(queue_name=queue_name, **kwds)
            assert queue.name == queue_name
            return queue

    def _build_queue(
        self,
        queue_name: str,
        **kwds: Unpack[QueueConfigOptions],
    ) -> MessageQueue:
        builder = self._get_queue_builder()
        return builder.build_queue(queue_name, **kwds)

    def _build_raw_queue(self, raw_queue: MessageQueue) -> MessageQueue:
        builder = self._get_queue_builder()
        return builder.setup_queue(raw_queue)

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
