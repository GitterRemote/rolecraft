import dataclasses
import typing
from collections.abc import Callable
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


@dataclasses.dataclass
class ConfigurableBrokerConfig[M_co]:
    queue_config: QueueConfig[M_co]
    queue_configs: dict[str, QueueConfig[M_co]] = dataclasses.field(
        default_factory=dict
    )

    def add_queue_config(
        self,
        queue_name: str,
        *,
        encoder: Encoder[M_co] | None = None,
        **kwds: Unpack[PartialQueueConfigOptions],
    ) -> typing.Self:
        if encoder:
            config = self.queue_config.replace(encoder=encoder, **kwds)
        else:
            config = self.queue_config.replace(**kwds)
        self.queue_configs[queue_name] = config
        return self


@dataclasses.dataclass(kw_only=True)
class InjectableConfig[Q: QueueConfig[Any] | IncompleteQueueConfig[Any]]:
    queue_configs: dict[str, QueueConfig[Any]] = dataclasses.field(
        default_factory=dict
    )
    broker_configs: dict[
        Broker[Any], ConfigurableBrokerConfig[Any]
    ] = dataclasses.field(default_factory=dict)
    queue_config: Q

    queue_names_by_broker: dict[Broker[Any], list[str]] = dataclasses.field(
        default_factory=dict
    )
    queue_to_broker: Callable[[str], Broker[Any] | None] | None = None

    @property
    def config_store_cls(self) -> type[ConfigStore]:
        return _config_store.DefaultConfigStore

    def inject(self):
        """Inject into the global config store."""
        self.create_config_store().set_as_defaut()

    def create_config_store(self) -> ConfigStore:
        config = self
        queue_configs = dict(config.queue_configs)
        broker_queue_configs: dict[Broker[Any], QueueConfig[Any]] = {}

        for broker, broker_config in config.broker_configs.items():
            queue_configs.update(broker_config.queue_configs)
            broker_queue_configs[broker] = broker_config.queue_config

        return self.config_store_cls(
            queue_config=config.queue_config,
            queue_configs=queue_configs,
            broker_queue_configs=broker_queue_configs,
            queue_to_broker=self.queue_to_broker,
            queue_names_by_broker=self.queue_names_by_broker,
        )


@dataclasses.dataclass
class ConfigurableDefaultConfig[M](InjectableConfig[QueueConfig[M]]):
    queue_config: QueueConfig[M]

    @typing.overload
    def add_queue_config(
        self,
        queue_name: str,
        *,
        broker: Broker[M] | None = None,
        encoder: Encoder[M] | None = None,
        **kwds: Unpack[PartialQueueConfigOptions],
    ) -> typing.Self:
        ...

    @typing.overload
    def add_queue_config[O](
        self,
        queue_name: str,
        *,
        broker: Broker[O],
        encoder: Encoder[O],
        **kwds: Unpack[PartialQueueConfigOptions],
    ) -> typing.Self:
        ...

    def add_queue_config(
        self,
        queue_name: str,
        *,
        broker: Broker[Any] | None = None,
        encoder: Encoder[Any] | None = None,
        **kwds: Unpack[PartialQueueConfigOptions],
    ) -> typing.Self:
        config = self.queue_config.replace(**kwds)
        if broker:
            config = config.replace(broker=broker)
        if encoder:
            config = config.replace(encoder=encoder)
        self.queue_configs[queue_name] = config
        return self

    @typing.overload
    def add_broker_config[O](
        self,
        broker: Broker[O],
        encoder: Encoder[O],
        **kwds: Unpack[PartialQueueConfigOptions],
    ) -> ConfigurableBrokerConfig[O]:
        ...

    @typing.overload
    def add_broker_config(
        self,
        broker: Broker[M],
        **kwds: Unpack[PartialQueueConfigOptions],
    ) -> ConfigurableBrokerConfig[M]:
        ...

    def add_broker_config[T](
        self,
        broker: Broker[T],
        encoder: Encoder[T] | None = None,
        **kwds: Unpack[PartialQueueConfigOptions],
    ) -> ConfigurableBrokerConfig[T] | ConfigurableBrokerConfig[M]:
        if encoder:
            config = self.queue_config.replace(
                broker=broker, encoder=encoder, **kwds
            )
            config = ConfigurableBrokerConfig(queue_config=config)
        elif self._assert(broker):
            config = self.queue_config.replace(broker=broker, **kwds)
            config = ConfigurableBrokerConfig(queue_config=config)
        else:
            raise TypeError("Unmatched broker type with encoder")

        self.broker_configs[broker] = config
        return config

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

    def update_default[M](
        self,
        *,
        broker: Broker[M],
        encoder: Encoder[M] | None = None,
        **kwds: Unpack[PartialQueueConfigOptions],
    ) -> (
        ConfigurableDefaultConfig[M]
        | ConfigurableDefaultConfig[HeaderBytesRawMessage]
    ):
        """Once update default, you should discard current config instance and use new returned config for later configuration"""
        if encoder:
            config = self.queue_config.replace(
                broker=broker, encoder=encoder, **kwds
            ).to_queue_config()
            return ConfigurableDefaultConfig(
                queue_config=config,
                queue_configs=self.queue_configs,
                broker_configs=self.broker_configs,
            )
        elif self._assert(broker):
            config = self.queue_config.replace(
                broker=broker, **kwds
            ).to_queue_config()
            return ConfigurableDefaultConfig(
                queue_config=config,
                queue_configs=self.queue_configs,
                broker_configs=self.broker_configs,
            )
        else:
            raise TypeError("Unmatched broker type with encoder")

    def _assert(
        self, broker: Broker[Any]
    ) -> typing.TypeGuard[Broker[HeaderBytesRawMessage]]:
        # TODO: implement it
        return True

    def add_broker_config[M](
        self,
        broker: Broker[M],
        encoder: Encoder[M],
        **kwds: Unpack[PartialQueueConfigOptions],
    ) -> ConfigurableBrokerConfig[M]:
        config = self.queue_config.replace(
            broker=broker, encoder=encoder, **kwds
        ).to_queue_config()
        broker_config = ConfigurableBrokerConfig(queue_config=config)
        self.broker_configs[broker] = broker_config
        return broker_config

    def add_queue_config[M](
        self,
        queue_name: str,
        broker: Broker[M] | Broker[HeaderBytesRawMessage],
        encoder: Encoder[M] | None = None,
        **kwds: Unpack[PartialQueueConfigOptions],
    ) -> typing.Self:
        if encoder:
            config = self.queue_config.replace(
                broker=broker, encoder=encoder, **kwds
            ).to_queue_config()
        else:
            config = self.queue_config.replace(
                broker=broker, **kwds
            ).to_queue_config()
        self.queue_configs[queue_name] = config
        return self
