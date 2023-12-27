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
            config = dataclasses.replace(
                self.queue_config, encoder=encoder, **kwds
            )
        else:
            config = dataclasses.replace(self.queue_config, **kwds)
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
            queue_to_broker=self._create_queue_to_broker_resolver(),
        )

    def _create_queue_to_broker_resolver(
        self,
    ) -> Callable[[str], Broker[Any] | None]:
        queue_broker_mapping: dict[str, Broker[Any]] = {}
        for broker, queue_names in self.queue_names_by_broker.items():
            for queue_name in queue_names:
                queue_broker_mapping[queue_name] = broker

        resolver = self.queue_to_broker

        def _resovler(queue_name: str) -> Broker[Any] | None:
            if broker := queue_broker_mapping.get(queue_name):
                return broker
            return resolver(queue_name) if resolver else None

        return _resovler


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
        config = dataclasses.replace(self.queue_config, **kwds)
        if broker:
            config = dataclasses.replace(config, broker=broker)
        if encoder:
            config = dataclasses.replace(config, broker=broker)
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
            config = dataclasses.replace(
                self.queue_config, broker=broker, encoder=encoder, **kwds
            )
            config = ConfigurableBrokerConfig(queue_config=config)
        elif self._assert(broker):
            config = dataclasses.replace(
                self.queue_config, broker=broker, **kwds
            )
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
            config = dataclasses.replace(
                self.queue_config, broker=broker, encoder=encoder, **kwds
            ).to_queue_config()
            return ConfigurableDefaultConfig(
                queue_config=config,
                queue_configs=self.queue_configs,
                broker_configs=self.broker_configs,
            )
        elif self._assert(broker):
            config = dataclasses.replace(
                self.queue_config, broker=broker, **kwds
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
        config = typing.cast(
            QueueConfig[M],
            dataclasses.replace(
                self.queue_config, broker=broker, encoder=encoder, **kwds
            ),
        )
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
            config = dataclasses.replace(
                self.queue_config, broker=broker, encoder=encoder, **kwds
            ).to_queue_config()
        else:
            config = dataclasses.replace(
                self.queue_config, broker=broker, **kwds
            ).to_queue_config()
        self.queue_configs[queue_name] = config
        return self
