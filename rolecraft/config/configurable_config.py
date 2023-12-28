import dataclasses
import functools
import typing
from collections import defaultdict
from typing import Any, TypeVar, Unpack

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
    queue_config: QueueConfig[M_co]
    queue_configs: dict[str, QueueConfig[M_co]] = dataclasses.field(
        default_factory=dict
    )

    def add_queue_config(
        self,
        queue_name: str,
        **kwds: Unpack[PartialQueueConfigOptions2[M_co]],
    ) -> typing.Self:
        config = self.queue_config.replace(**kwds)
        self.queue_configs[queue_name] = config
        return self


@dataclasses.dataclass(kw_only=True)
class InjectableConfig[Q: QueueConfig[Any] | IncompleteQueueConfig[Any]]:
    queue_config: Q

    # Include queue specific QueueConfigs that are based on default QueueConfig even the broker is not the default broker.
    queue_configs: dict[
        Broker[Any], dict[str, QueueConfig[Any]]
    ] = dataclasses.field(
        default_factory=functools.partial(
            defaultdict[Broker[Any], dict[str, QueueConfig[Any]]], dict
        )
    )

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
        }
        if not isinstance(self.queue_config, IncompleteQueueConfig):
            broker_queue_config[self.queue_config.broker] = self.queue_config

        broker_queue_configs = {
            broker: broker_config.queue_configs
            for broker, broker_config in self.broker_configs.items()
        }

        for broker, queue_configs in self.queue_configs.items():
            existing_queue_configs = broker_queue_configs.get(broker)
            if existing_queue_configs:
                existing_queue_configs.update(queue_configs)
            else:
                broker_queue_configs[broker] = queue_configs

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

        self.queue_configs[config.broker][queue_name] = config
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
        encoder: Encoder[T],
        **kwds: Unpack[PartialQueueConfigOptions],
    ) -> ConfigurableBrokerConfig[T]:
        ...

    def add_broker_config[T](
        self,
        broker: Broker[T],
        encoder: Encoder[T] | None = None,
        **kwds: Unpack[PartialQueueConfigOptions],
    ) -> ConfigurableBrokerConfig[T] | ConfigurableBrokerConfig[M]:
        """Add default QueueConfig for a broker, based on current default QueueConfig"""
        if encoder:
            config = self.queue_config.replace(
                broker=broker, encoder=encoder, **kwds
            )
            broker_config = ConfigurableBrokerConfig(queue_config=config)
        elif self._assert(broker):
            config = self.queue_config.replace(broker=broker, **kwds)
            broker_config = ConfigurableBrokerConfig(queue_config=config)
        else:
            raise TypeError("Unmatched broker type with encoder")

        if broker in self.broker_configs:
            raise ValueError("Broker default QueueConfig exists")

        self.broker_configs[broker] = broker_config
        return broker_config

    def _assert(self, broker: Broker[Any]) -> typing.TypeGuard[Broker[M]]:
        # TODO: implement it
        return True


@dataclasses.dataclass
class ConfigurableConfig(
    InjectableConfig[IncompleteQueueConfig[HeaderBytesRawMessage]]
):
    queue_config: IncompleteQueueConfig[
        HeaderBytesRawMessage
    ] = IncompleteQueueConfig.default()

    def update_default[T](
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
            queue_configs=self.queue_configs,
            broker_configs=self.broker_configs,
        )

    def add_broker_config[T](
        self,
        broker: Broker[T],
        encoder: Encoder[T],
        **kwds: Unpack[PartialQueueConfigOptions],
    ) -> ConfigurableBrokerConfig[T]:
        if broker in self.broker_configs:
            raise ValueError("Broker default QueueConfig exists")

        config = self.queue_config.replace(
            broker=broker, encoder=encoder, **kwds
        ).to_queue_config()
        broker_config = ConfigurableBrokerConfig(queue_config=config)
        self.broker_configs[broker] = broker_config
        return broker_config

    def add_queue_config[T](
        self,
        queue_name: str,
        broker: Broker[T],
        **kwds: Unpack[PartialQueueConfigOptions2[T]],
    ) -> typing.Self:
        # if not kwds.get("encoder") and not self._assert(broker):
        #     raise TypeError(
        #         "If no encoder, the broker should be compatible with HeaderBytesRawMessage type"
        #     )
        config = self.queue_config.replace(
            broker=broker, **kwds
        ).to_queue_config()

        self.queue_configs[broker][queue_name] = config
        return self

    def _assert(
        self, broker: Broker[Any]
    ) -> typing.TypeGuard[Broker[HeaderBytesRawMessage]]:
        # TODO: implement it
        return True
