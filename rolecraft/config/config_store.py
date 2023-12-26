import abc
import dataclasses
from typing import Any, Protocol, Self, Unpack

from rolecraft.broker import Broker

from .queue_config import (
    IncompleteQueueConfig,
    QueueConfig,
    AllQueueConfigKeys,
)


class ConfigFetcher(Protocol):
    def __call__[M](
        self,
        queue_name: str | None = None,
        **kwds: Unpack[AllQueueConfigKeys[M]],
    ) -> QueueConfig[M]:
        ...


class IncompleteConfigError(Exception):
    pass


class ConfigStore(abc.ABC):
    """It stores all default QueueConfig for the initialization of the MessageQueue object. Besides, it stores queue-specific or broker-specific QueueConfig."""

    QueueConfigType = QueueConfig[Any] | IncompleteQueueConfig[Any]
    QueueConfigsType = dict[str, QueueConfig[Any]]
    BrokerQueueConfigsType = dict[Broker[Any], QueueConfig[Any]]

    @abc.abstractmethod
    def __init__(
        self,
        queue_config: QueueConfigType | None = None,
        queue_configs: dict[str, QueueConfig[Any]] | None = None,
        broker_queue_configs: BrokerQueueConfigsType | None = None,
    ) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def set_as_defaut(self) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def fetcher(self) -> ConfigFetcher:
        raise NotImplementedError


@dataclasses.dataclass(kw_only=True)
class DefaultConfigStore(ConfigStore, ConfigFetcher):
    queue_config: ConfigStore.QueueConfigType
    queue_configs: ConfigStore.QueueConfigsType = dataclasses.field(
        default_factory=dict
    )
    broker_queue_configs: ConfigStore.BrokerQueueConfigsType = (
        dataclasses.field(default_factory=dict)
    )

    @classmethod
    def set(cls, store: Self):
        """set the store to the global variable as a default store"""
        global default
        default = store

    @classmethod
    def get(cls) -> "ConfigStore | None":
        global default
        return default

    def set_as_defaut(self):
        self.set(self)

    @property
    def fetcher(self):
        return self

    def __call__[O](
        self,
        queue_name: str | None = None,
        **kwds: Unpack[AllQueueConfigKeys[O]],
    ) -> QueueConfig[O] | QueueConfig[Any]:
        # TODO: raise ValueError if broker/encoder/middlewares are set to None
        config = self._get_default_queue_config(
            queue_name, broker=kwds.get("broker")
        )

        if kwds:
            config = dataclasses.replace(config, **kwds)

        return config

    def _get_default_queue_config(
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
                        raise IncompleteConfigError()

                    config = dataclasses.replace(
                        config, broker=broker
                    ).to_queue_config()

        return config


default: ConfigStore | None = None
