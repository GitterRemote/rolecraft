import abc
import dataclasses
from collections.abc import Callable
from typing import Any, Self, Unpack

from rolecraft.broker import Broker
from rolecraft.queue_factory.config_fetcher import (
    QueueConfigOptions,
    ConfigFetcher,
)

from .queue_config import IncompleteQueueConfig, QueueConfig


class IncompleteConfigError(Exception):
    pass


class ConfigStore(abc.ABC):
    """It stores all default QueueConfig for the initialization of the MessageQueue object. Besides, it stores queue-specific or broker-specific QueueConfig."""

    QueueConfigType = QueueConfig[Any] | IncompleteQueueConfig[Any]
    QueueConfigsType = dict[str, QueueConfig[Any]]
    BrokerQueueConfigsType = dict[Broker[Any], QueueConfig[Any]]
    QueueToBrokerType = Callable[[str], Broker[Any] | None]

    @abc.abstractmethod
    def __init__(
        self,
        queue_config: QueueConfigType | None = None,
        queue_configs: dict[str, QueueConfig[Any]] | None = None,
        broker_queue_configs: BrokerQueueConfigsType | None = None,
        queue_to_broker: QueueToBrokerType | None = None,
    ) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def set_as_defaut(self) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def fetcher(self) -> ConfigFetcher:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def queue_names_with_broker(self) -> dict[str, Broker | None]:
        """queue names from queue_configs and queue_to_broker configuration."""
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
    queue_to_broker: ConfigStore.QueueToBrokerType = dict[str, None]().get

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
        **kwds: Unpack[QueueConfigOptions[O]],
    ) -> QueueConfig[O] | QueueConfig[Any]:
        # TODO: raise ValueError if broker/encoder/middlewares are set to None
        broker = kwds.get("broker")

        if not broker and queue_name is not None:
            broker = self.queue_to_broker(queue_name)

        config = self._get_default_queue_config(queue_name, broker=broker)

        if kwds:
            config = dataclasses.replace(config, **kwds)

        return config

    def _get_default_queue_config(
        self,
        queue_name: str | None,
        broker: Broker[Any] | None,
    ) -> QueueConfig[Any]:
        if queue_name and (config := self.queue_configs.get(queue_name)):
            return config

        if broker and (config := self.broker_queue_configs.get(broker)):
            return config

        if isinstance(self.queue_config, IncompleteQueueConfig):
            if not broker:
                raise IncompleteConfigError()

            return dataclasses.replace(
                self.queue_config, broker=broker
            ).to_queue_config()

        return self.queue_config


default: ConfigStore | None = None
