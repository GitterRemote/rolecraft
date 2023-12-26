import dataclasses
import typing

from . import queue_builder as _queue_builder
from . import config as _config
from .broker import Broker
from .config import ConfigStore
from .queue import MessageQueue


@dataclasses.dataclass(kw_only=True)
class QueueManager:
    """It accepts user-defined queue related configurations, which are used during the queue building stage."""

    queue_names: list[str] | None = None
    queue_names_per_broker: dict[Broker, list[str]] | None = None
    raw_queues: list[MessageQueue] | None = None
    config_store: ConfigStore | None = None

    def __post_init__(self) -> None:
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
        broker: Broker | None = None,
    ) -> MessageQueue:
        ...

    def get_or_bulid(
        self,
        *,
        queue_name: str | None = None,
        broker: Broker | None = None,
        raw_queue: MessageQueue | None = None,
    ) -> MessageQueue:
        """Always return the same Queue instance if it is the same name."""
        if raw_queue:
            if queue_name is not None or broker:
                raise ValueError("No other parameters for raw queue")
            queue_name = raw_queue.name
        elif queue_name is None:
            raise ValueError("queue or queue name should be specified")

        if queue_name in self._queues:
            return self._queues[queue_name]

        # TODO: add thread lock

        builder = self._get_queue_builder()
        if raw_queue:
            queues = builder.build(raw_queues=[raw_queue])
        elif broker:
            queues = builder.build(
                queue_names_per_broker={broker: [queue_name]}
            )
        else:
            queues = builder.build(queue_names=[queue_name])

        assert len(queues) == 1
        queue = queues[0]
        self._queues[queue_name] = queue
        return queue

    def build_queues(self) -> list[MessageQueue]:
        """Always build brand new queues from the latest configuration."""
        builder = self._get_queue_builder()
        return builder.build(
            queue_names=self.queue_names,
            queue_names_per_broker=self.queue_names_per_broker,
            raw_queues=self.raw_queues,
        )

    def _get_queue_builder(self):
        return _queue_builder.QueueBuilder(
            config_fetcher=self._get_config_store().fetcher
        )

    def _get_config_store(self) -> ConfigStore:
        store = self.config_store or _config.DefaultConfigStore.get()
        assert store
        return store
