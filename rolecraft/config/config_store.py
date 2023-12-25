import dataclasses
from typing import Any, Protocol, Self, Unpack

from rolecraft.broker import Broker
from rolecraft.encoder import Encoder

from .queue_config import (
    IncompleteQueueConfig,
    QueueConfig,
    QueueConfigKeys,
)


class ConfigFetcher(Protocol):
    def __call__[M](
        self,
        queue_name: str | None = None,
        *,
        broker: Broker[M] | None = None,
        encoder: Encoder[M] | None = None,
        **kwds: Unpack[QueueConfigKeys],
    ) -> QueueConfig[M]:
        ...


@dataclasses.dataclass
class ConfigStore(ConfigFetcher):
    queue_configs: dict[str, QueueConfig[Any]]
    broker_queue_configs: dict[Broker[Any], QueueConfig[Any]]
    queue_config: QueueConfig[Any] | IncompleteQueueConfig[Any]

    @classmethod
    def set(cls, store: Self):
        """set the store to the global variable as a default store"""
        global default
        default = store

    @classmethod
    def get(cls) -> "ConfigStore | None":
        global default
        return default

    def set_defaut(self):
        self.set(self)

    @property
    def default_queue_config(self) -> QueueConfig[Any]:
        if isinstance(self.queue_config, QueueConfig):
            return self.queue_config
        raise RuntimeError("Default config is not configured")

    def __call__[O](
        self,
        queue_name: str | None = None,
        *,
        broker: Broker[O] | None = None,
        encoder: Encoder[O] | None = None,
        **kwds: Unpack[QueueConfigKeys],
    ) -> QueueConfig[O] | QueueConfig[Any]:
        config = self._get_queue_config(queue_name, broker)

        if kwds:
            config = dataclasses.replace(config, **kwds)
        if broker:
            config = dataclasses.replace(config, broker=broker)
        if encoder:
            config = dataclasses.replace(config, broker=broker)

        return config

    def _get_queue_config(
        self,
        queue_name: str | None,
        broker: Broker[Any] | None,
    ) -> QueueConfig[Any]:
        config = self.queue_configs.get(queue_name) if queue_name else None

        if not config:
            if broker:
                config = self.broker_queue_configs.get(broker)

            if not config:
                config = self.queue_config
                if not isinstance(config, QueueConfig):
                    if not broker:
                        raise RuntimeError("No default broker is set")
                    config = dataclasses.replace(
                        config, broker=broker
                    ).to_queue_config()

        return config


default: ConfigStore | None = None
