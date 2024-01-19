import dataclasses
from collections.abc import Mapping, Sequence
from typing import Any, TypedDict, TypeVar, Unpack

from rolecraft.broker import Broker
from rolecraft.encoder import Encoder
from rolecraft.middleware import Middleware

M_co = TypeVar("M_co", covariant=True)


class PartialQueueConfigOptions(TypedDict, total=False):
    middlewares: Sequence[Middleware]
    wait_time_seconds: int | None
    settings: Mapping[str, Any]


class QueueConfigOptions[M](PartialQueueConfigOptions, total=False):
    encoder: Encoder[M]
    broker: Broker[M]


@dataclasses.dataclass(kw_only=True, frozen=True)
class IncompleteQueueConfig[M_co]:
    encoder: Encoder[M_co]
    broker: Broker[M_co] | None = None

    middlewares: Sequence[Middleware] = dataclasses.field(default_factory=list)
    wait_time_seconds: int | None = None
    settings: Mapping[str, Any] = dataclasses.field(default_factory=dict)

    def replace(self, **kwds: Unpack[QueueConfigOptions[Any]]):
        return dataclasses.replace(self, **kwds)


@dataclasses.dataclass(kw_only=True, frozen=True)
class QueueConfig[M_co](IncompleteQueueConfig[M_co]):
    broker: Broker[M_co]

    def replace(self, **kwds: Unpack[QueueConfigOptions[Any]]):
        return dataclasses.replace(self, **kwds)

    @classmethod
    def create_from[T](
        cls,
        incomplete_queue_config: IncompleteQueueConfig[T],
        broker: Broker[T],
    ) -> "QueueConfig[T]":
        data = {
            f.name: getattr(incomplete_queue_config, f.name)
            for f in dataclasses.fields(cls)
        }
        data["broker"] = broker
        config = cls(**data)
        return config  # type: ignore
