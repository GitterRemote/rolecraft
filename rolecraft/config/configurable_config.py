import dataclasses
import typing
from typing import Any, Self, TypeVar, Unpack

from rolecraft.broker import Broker, HeaderBytesRawMessage
from rolecraft.encoder import Encoder
from rolecraft.queue_config import (
    IncompleteQueueConfig,
    PartialQueueConfigOptions,
    QueueConfig,
    QueueConfigOptions,
)

from . import config_store as _config_store
from . import default_queue_config as _default_queue_config
from . import global_default as _global_default
from .config_store import ConfigStore
from .middleware_list import MiddlewareList

M_co = TypeVar("M_co", covariant=True)


class NoBrokerQueueConfigOptions[M](PartialQueueConfigOptions, total=False):
    encoder: Encoder[M]


@dataclasses.dataclass(kw_only=True, frozen=True)
class ConfigurableIncompleteQueueConfig[M_co](IncompleteQueueConfig[M_co]):
    middlewares: MiddlewareList = dataclasses.field(
        default_factory=MiddlewareList
    )

    @typing.overload
    def replace(self, **kwds: Unpack[PartialQueueConfigOptions]) -> Self:
        ...

    @typing.overload
    def replace[T](
        self,
        *,
        encoder: Encoder[T],
        broker: Broker[T] | None = None,
        **kwds: Unpack[PartialQueueConfigOptions],
    ) -> "ConfigurableIncompleteQueueConfig[T]":
        ...

    @typing.overload
    def replace(self, **kwds: Unpack[QueueConfigOptions[M_co]]) -> Self:
        ...

    def replace(self, **kwds: Unpack[QueueConfigOptions[Any]]):  # type: ignore
        if "middlewares" in kwds and not isinstance(
            kwds["middlewares"], MiddlewareList
        ):
            kwds["middlewares"] = MiddlewareList(kwds["middlewares"])
        return super().replace(**kwds)

    def to_queue_config(self) -> "ConfigurableQueueConfig[M_co]":
        assert self.broker
        return ConfigurableQueueConfig(
            **{f.name: getattr(self, f.name) for f in dataclasses.fields(self)}
        )

    @classmethod
    def default(
        cls,
    ) -> Self:
        default = _default_queue_config.DefaultQueueConfig()
        return cls(
            **{
                f.name: getattr(default, f.name)
                for f in dataclasses.fields(cls)
            }
        )


@dataclasses.dataclass(kw_only=True, frozen=True)
class ConfigurableQueueConfig[M_co](QueueConfig[M_co]):
    middlewares: MiddlewareList = dataclasses.field(
        default_factory=MiddlewareList
    )

    @typing.overload
    def replace(self, **kwds: Unpack[PartialQueueConfigOptions]) -> Self:
        ...

    @typing.overload
    def replace[T](
        self,
        encoder: Encoder[T],
        broker: Broker[T],
        **kwds: Unpack[PartialQueueConfigOptions],
    ) -> "ConfigurableQueueConfig[T]":
        ...

    @typing.overload
    def replace(self, **kwds: Unpack[QueueConfigOptions[M_co]]) -> Self:
        ...

    def replace(self, **kwds: Unpack[QueueConfigOptions[Any]]):  # type: ignore
        if "middlewares" in kwds and not isinstance(
            kwds["middlewares"], MiddlewareList
        ):
            kwds["middlewares"] = MiddlewareList(kwds["middlewares"])
        return super().replace(**kwds)


@dataclasses.dataclass
class ConfigurableBrokerConfig[M_co]:
    default: QueueConfig[M_co] | None = None
    queue_configs: dict[str, QueueConfig[M_co]] = dataclasses.field(
        default_factory=dict
    )

    def add_queue_config(
        self,
        queue_name: str,
        **kwds: Unpack[NoBrokerQueueConfigOptions[M_co]],
    ) -> typing.Self:
        if not self.default:
            raise RuntimeError("No defualt QueueConfig for the broker")
        config = self.default.replace(**kwds)
        self.queue_configs[queue_name] = config
        return self

    def insert_queue_config(
        self, queue_name: str, queue_config: QueueConfig[M_co]
    ) -> typing.Self:
        if self.default:
            assert queue_config.broker is self.default.broker
        self.queue_configs[queue_name] = queue_config
        return self

    @classmethod
    def new(cls, broker: Broker[M_co]) -> Self:
        """auxiliary method for typing"""
        return cls()


@dataclasses.dataclass(kw_only=True)
class InjectableConfig[Q: QueueConfig[Any] | IncompleteQueueConfig[Any]]:
    default: Q

    broker_configs: dict[
        Broker[Any], ConfigurableBrokerConfig[Any]
    ] = dataclasses.field(default_factory=dict)

    queue_names_by_broker: dict[Broker[Any], list[str]] = dataclasses.field(
        default_factory=dict
    )

    @property
    def config_store_cls(self) -> type[ConfigStore]:
        return _config_store.SimpleConfigStore

    def inject(self):
        """Inject into the global config store."""
        _global_default.global_default.set(self.create_config_store())

    def create_config_store(self) -> ConfigStore:
        broker_queue_config = {
            broker: broker_config.default
            for broker, broker_config in self.broker_configs.items()
            if broker_config.default
        }

        broker_queue_configs = {
            broker: broker_config.queue_configs
            for broker, broker_config in self.broker_configs.items()
            if broker_config.queue_configs
        }

        return self.config_store_cls(
            queue_config=self.default,
            broker_queue_config=broker_queue_config,
            broker_queue_configs=broker_queue_configs,
            queue_names_by_broker=self.queue_names_by_broker,
        )


@dataclasses.dataclass
class ConfigurableDefaultConfig[M](
    InjectableConfig[ConfigurableQueueConfig[M]]
):
    default: ConfigurableQueueConfig[M]

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
        broker = broker or self.default.broker
        if broker_config := self.broker_configs.get(broker):
            return broker_config
        broker_config = ConfigurableBrokerConfig.new(broker)
        self.broker_configs[broker] = broker_config
        return broker_config

    @typing.overload
    def add_queue_config(
        self,
        queue_name: str,
        **kwds: Unpack[NoBrokerQueueConfigOptions[M]],
    ) -> typing.Self:
        ...

    @typing.overload
    def add_queue_config(
        self,
        queue_name: str,
        *,
        broker: Broker[M],
        **kwds: Unpack[NoBrokerQueueConfigOptions[M]],
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
        **kwds: Unpack[NoBrokerQueueConfigOptions[Any]],
    ) -> typing.Self:
        """It will create the queue specific QueueConfig based on default QueueConfig"""
        if not broker:
            config = self.default.replace(**kwds)
        else:
            config = self.default.replace(broker=broker, **kwds)
        self._get_broker_config(broker).insert_queue_config(queue_name, config)
        return self

    @typing.overload
    def add_broker_config(
        self,
        broker: Broker[M],
        **kwds: Unpack[NoBrokerQueueConfigOptions[M]],
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
        **kwds: Unpack[NoBrokerQueueConfigOptions[T]],
    ) -> ConfigurableBrokerConfig[T]:
        """Add default QueueConfig for a broker, based on current default QueueConfig"""
        broker_config = self._get_broker_config(broker)
        assert not broker_config.default
        config = self.default.replace(broker=broker, **kwds)
        broker_config.default = config
        return broker_config


@dataclasses.dataclass
class ConfigurableConfig(
    InjectableConfig[ConfigurableIncompleteQueueConfig[HeaderBytesRawMessage]]
):
    default: ConfigurableIncompleteQueueConfig[
        HeaderBytesRawMessage
    ] = dataclasses.field(
        default_factory=ConfigurableIncompleteQueueConfig.default
    )

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
        **kwds: Unpack[NoBrokerQueueConfigOptions[T]],
    ) -> (
        ConfigurableDefaultConfig[T]
        | ConfigurableDefaultConfig[HeaderBytesRawMessage]
    ):
        """Once update default, you should discard current config instance and use new returned config for later configuration"""
        config = self.default.replace(broker=broker, **kwds).to_queue_config()

        return ConfigurableDefaultConfig(
            default=config,
            broker_configs=self.broker_configs,
        )

    def add_broker_config[T](
        self,
        broker: Broker[T],
        encoder: Encoder[T],
        **kwds: Unpack[PartialQueueConfigOptions],
    ) -> ConfigurableBrokerConfig[T]:
        broker_config = self._get_broker_config(broker)
        assert not broker_config.default

        config = self.default.replace(
            broker=broker, encoder=encoder, **kwds
        ).to_queue_config()
        broker_config.default = config
        return broker_config

    def add_queue_config[T](
        self,
        queue_name: str,
        broker: Broker[T],
        **kwds: Unpack[NoBrokerQueueConfigOptions[T]],
    ) -> typing.Self:
        broker_config = self._get_broker_config(broker)
        config = self.default.replace(broker=broker, **kwds).to_queue_config()

        broker_config.insert_queue_config(queue_name, config)
        return self
