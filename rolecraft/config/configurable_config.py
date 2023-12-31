import dataclasses
import typing
from typing import Any, Self, TypeVar, Unpack

from rolecraft.broker import Broker, HeaderBytesRawMessage
from rolecraft.encoder import Encoder

from . import config_store as _config_store
from .config_store import ConfigStore
from .queue_config import (
    IncompleteQueueConfig,
    PartialQueueConfigOptions,
    QueueConfig,
)

M_co = TypeVar("M_co", covariant=True)


class PartialQueueConfigOptions2[M](PartialQueueConfigOptions, total=False):
    encoder: Encoder[M]


@dataclasses.dataclass
class ConfigurableBrokerConfig[M_co]:
    queue_config: QueueConfig[M_co] | None = None
    queue_configs: dict[str, QueueConfig[M_co]] = dataclasses.field(
        default_factory=dict
    )

    def add_queue_config(
        self,
        queue_name: str,
        **kwds: Unpack[PartialQueueConfigOptions2[M_co]],
    ) -> typing.Self:
        if not self.queue_config:
            raise RuntimeError("No defualt QueueConfig for the broker")
        config = self.queue_config.replace(**kwds)
        self.queue_configs[queue_name] = config
        return self

    def insert_queue_config(
        self, queue_name: str, queue_config: QueueConfig[M_co]
    ) -> typing.Self:
        if self.queue_config:
            assert queue_config.broker is self.queue_config.broker
        self.queue_configs[queue_name] = queue_config
        return self

    @classmethod
    def new(cls, broker: Broker[M_co]) -> Self:
        """auxiliary method for typing"""
        return cls()


@dataclasses.dataclass(kw_only=True)
class InjectableConfig[Q: QueueConfig[Any] | IncompleteQueueConfig[Any]]:
    queue_config: Q

    broker_configs: dict[
        Broker[Any], ConfigurableBrokerConfig[Any]
    ] = dataclasses.field(default_factory=dict)

    queue_names_by_broker: dict[Broker[Any], list[str]] = dataclasses.field(
        default_factory=dict
    )

    @property
    def config_store_cls(self) -> type[ConfigStore]:
        return _config_store.DefaultConfigStore

    def inject(self):
        """Inject into the global config store."""
        self.create_config_store().set_as_defaut()

    def create_config_store(self) -> ConfigStore:
        broker_queue_config = {
            broker: broker_config.queue_config
            for broker, broker_config in self.broker_configs.items()
            if broker_config.queue_config
        }

        broker_queue_configs = {
            broker: broker_config.queue_configs
            for broker, broker_config in self.broker_configs.items()
            if broker_config.queue_configs
        }

        return self.config_store_cls(
            queue_config=self.queue_config,
            broker_queue_config=broker_queue_config,
            broker_queue_configs=broker_queue_configs,
            queue_names_by_broker=self.queue_names_by_broker,
        )


@dataclasses.dataclass
class ConfigurableDefaultConfig[M](InjectableConfig[QueueConfig[M]]):
    queue_config: QueueConfig[M]

    @typing.overload
    def _get_broker_config(
        self, broker: None = None
    ) -> ConfigurableBrokerConfig[M]:
        ...

    @typing.overload
    def _get_broker_config[T](
        self, broker: Broker[T]
    ) -> ConfigurableBrokerConfig[T]:
        ...

    def _get_broker_config(
        self, broker: Broker[Any] | None = None
    ) -> ConfigurableBrokerConfig[Any]:
        """Get or create the BrokerConfig"""
        broker = broker or self.queue_config.broker
        if broker_config := self.broker_configs.get(broker):
            return broker_config
        broker_config = ConfigurableBrokerConfig.new(broker)
        self.broker_configs[broker] = broker_config
        return broker_config

    @typing.overload
    def add_queue_config(
        self,
        queue_name: str,
        **kwds: Unpack[PartialQueueConfigOptions2[M]],
    ) -> typing.Self:
        ...

    @typing.overload
    def add_queue_config(
        self,
        queue_name: str,
        *,
        broker: Broker[M],
        **kwds: Unpack[PartialQueueConfigOptions2[M]],
    ) -> typing.Self:
        ...

    @typing.overload
    def add_queue_config[T](
        self,
        queue_name: str,
        *,
        broker: Broker[T],
        encoder: Encoder[T],
        **kwds: Unpack[PartialQueueConfigOptions],
    ) -> typing.Self:
        ...

    def add_queue_config(
        self,
        queue_name: str,
        *,
        broker: Broker[Any] | None = None,
        **kwds: Unpack[PartialQueueConfigOptions2[Any]],
    ) -> typing.Self:
        """It will create the queue specific QueueConfig based on default QueueConfig"""
        if not broker:
            config = self.queue_config.replace(**kwds)
        else:
            config = self.queue_config.replace(broker=broker, **kwds)
        self._get_broker_config(broker).insert_queue_config(queue_name, config)
        return self

    @typing.overload
    def add_broker_config(
        self,
        broker: Broker[M],
        **kwds: Unpack[PartialQueueConfigOptions2[M]],
    ) -> ConfigurableBrokerConfig[M]:
        ...

    @typing.overload
    def add_broker_config[T](
        self,
        broker: Broker[T],
        *,
        encoder: Encoder[T],
        **kwds: Unpack[PartialQueueConfigOptions],
    ) -> ConfigurableBrokerConfig[T]:
        ...

    def add_broker_config[T](
        self,
        broker: Broker[T],
        **kwds: Unpack[PartialQueueConfigOptions2[T]],
    ) -> ConfigurableBrokerConfig[T]:
        """Add default QueueConfig for a broker, based on current default QueueConfig"""
        broker_config = self._get_broker_config(broker)
        assert not broker_config.queue_config
        config = self.queue_config.replace(broker=broker, **kwds)
        broker_config.queue_config = config
        return broker_config


@dataclasses.dataclass
class ConfigurableConfig(
    InjectableConfig[IncompleteQueueConfig[HeaderBytesRawMessage]]
):
    queue_config: IncompleteQueueConfig[
        HeaderBytesRawMessage
    ] = IncompleteQueueConfig.default()

    def _get_broker_config[T](
        self, broker: Broker[T]
    ) -> ConfigurableBrokerConfig[T]:
        if broker_config := self.broker_configs.get(broker):
            return broker_config
        broker_config = ConfigurableBrokerConfig.new(broker)
        self.broker_configs[broker] = broker_config
        return broker_config

    def set_default[T](
        self,
        *,
        broker: Broker[T],
        **kwds: Unpack[PartialQueueConfigOptions2[T]],
    ) -> (
        ConfigurableDefaultConfig[T]
        | ConfigurableDefaultConfig[HeaderBytesRawMessage]
    ):
        """Once update default, you should discard current config instance and use new returned config for later configuration"""
        config = self.queue_config.replace(
            broker=broker, **kwds
        ).to_queue_config()

        return ConfigurableDefaultConfig(
            queue_config=config,
            broker_configs=self.broker_configs,
        )

    def add_broker_config[T](
        self,
        broker: Broker[T],
        encoder: Encoder[T],
        **kwds: Unpack[PartialQueueConfigOptions],
    ) -> ConfigurableBrokerConfig[T]:
        broker_config = self._get_broker_config(broker)
        assert not broker_config.queue_config

        config = self.queue_config.replace(
            broker=broker, encoder=encoder, **kwds
        ).to_queue_config()
        broker_config.queue_config = config
        return broker_config

    def add_queue_config[T](
        self,
        queue_name: str,
        broker: Broker[T],
        **kwds: Unpack[PartialQueueConfigOptions2[T]],
    ) -> typing.Self:
        broker_config = self._get_broker_config(broker)
        config = self.queue_config.replace(
            broker=broker, **kwds
        ).to_queue_config()

        broker_config.insert_queue_config(queue_name, config)
        return self
