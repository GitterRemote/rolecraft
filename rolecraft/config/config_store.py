import abc
import dataclasses
from collections import defaultdict
from collections.abc import Mapping
from typing import Any, Unpack

from rolecraft.broker import Broker
from rolecraft.queue_config import IncompleteQueueConfig, QueueConfig
from rolecraft.queue_factory.config_fetcher import (
    ConfigFetcher,
    QueueConfigOptions,
)


class IncompleteConfigError(Exception):
    pass


class ConfigStore(abc.ABC):
    """It stores all default QueueConfig for the initialization of the MessageQueue object. Besides, it stores queue-specific or broker-specific QueueConfig."""

    QueueConfigType = QueueConfig[Any] | IncompleteQueueConfig[Any]
    QueueConfigsType = Mapping[str, QueueConfig[Any]]
    BrokerQueueConfigType = Mapping[Broker[Any], QueueConfig[Any]]
    BrokerQueueConfigsType = Mapping[Broker[Any], QueueConfigsType]

    QueueNamesByBrokerType = Mapping[Broker[Any], list[str]]

    @abc.abstractmethod
    def __init__(
        self,
        queue_config: QueueConfigType | None = None,
        broker_queue_config: BrokerQueueConfigType | None = None,
        broker_queue_configs: BrokerQueueConfigsType | None = None,
        queue_names_by_broker: QueueNamesByBrokerType | None = None,
    ) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def fetcher(self) -> ConfigFetcher:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def parsed_queue_names_by_broker(self) -> QueueNamesByBrokerType:
        """queue names from queue_names_by_broker if exists otherwise parsed from queue_configs"""
        raise NotImplementedError


@dataclasses.dataclass(kw_only=True)
class SimpleConfigStore(ConfigStore, ConfigFetcher):
    queue_config: ConfigStore.QueueConfigType
    broker_queue_config: ConfigStore.BrokerQueueConfigType = dataclasses.field(
        default_factory=dict
    )
    broker_queue_configs: ConfigStore.BrokerQueueConfigsType = (
        dataclasses.field(default_factory=dict)
    )

    queue_names_by_broker: ConfigStore.QueueNamesByBrokerType = (
        dataclasses.field(default_factory=dict)
    )

    @property
    def parsed_queue_names_by_broker(
        self,
    ) -> ConfigStore.QueueNamesByBrokerType:
        if self.queue_names_by_broker:
            return self.queue_names_by_broker

        queue_names = defaultdict[Broker[Any], list[str]](list)
        for broker, queue_configs in self.broker_queue_configs.items():
            for queue_name, queue_config in queue_configs.items():
                assert broker is queue_config.broker
                queue_names[broker].append(queue_name)

        return queue_names

    @property
    def fetcher(self):
        return self

    # ConfigFetcher implementation
    def __call__[O](
        self,
        queue_name: str | None = None,
        **kwds: Unpack[QueueConfigOptions[O]],
    ) -> QueueConfig[O] | QueueConfig[Any]:
        # TODO: raise ValueError if broker/encoder/middlewares are set to None

        config = self._get_default_queue_config(queue_name, kwds.get("broker"))

        if kwds:
            config = config.replace(**kwds)

        return config

    def _get_default_queue_config(
        self,
        queue_name: str | None,
        broker: Broker[Any] | None,
    ) -> QueueConfig[Any]:
        if broker:
            if queue_name is not None and (
                config := self.broker_queue_configs.get(broker, {}).get(
                    queue_name
                )
            ):
                return config
            if config := self.broker_queue_config.get(broker):
                return config
        elif queue_name is not None and self.queue_config.broker:
            if config := self.broker_queue_configs.get(
                self.queue_config.broker, {}
            ).get(queue_name):
                return config

        if not isinstance(self.queue_config, QueueConfig):
            if not broker:
                raise IncompleteConfigError()

            return QueueConfig.create_from(self.queue_config, broker)

        return self.queue_config


global_config_store: SimpleConfigStore
