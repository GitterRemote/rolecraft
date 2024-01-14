import dataclasses
import typing
from collections.abc import Sequence
from typing import Any, Self, TypedDict, TypeVar, Unpack

from rolecraft.broker import Broker
from rolecraft.encoder import Encoder
from rolecraft.middleware import Middleware, MiddlewareList

M_co = TypeVar("M_co", covariant=True)


class PartialQueueConfigOptions(TypedDict, total=False):
    middlewares: Sequence[Middleware] | MiddlewareList
    wait_time_seconds: int | None
    settings: dict[str, Any]


class QueueConfigOptions[M](PartialQueueConfigOptions, total=False):
    encoder: Encoder[M]
    broker: Broker[M]


@dataclasses.dataclass(kw_only=True, frozen=True)
class PartialQueueConfig:
    middlewares: Sequence[Middleware] | MiddlewareList = dataclasses.field(
        default_factory=MiddlewareList
    )
    wait_time_seconds: int | None = None
    settings: dict[str, Any] = dataclasses.field(default_factory=dict)

    def replace(self, **kwds: Unpack[PartialQueueConfigOptions]) -> Self:
        return dataclasses.replace(self, **kwds)


@dataclasses.dataclass(kw_only=True, frozen=True)
class QueueConfig[M_co](PartialQueueConfig):
    encoder: Encoder[M_co]
    broker: Broker[M_co]

    @typing.overload
    def replace(self, **kwds: Unpack[PartialQueueConfigOptions]) -> Self:
        ...

    @typing.overload
    def replace[T](
        self,
        encoder: Encoder[T],
        broker: Broker[T],
        **kwds: Unpack[PartialQueueConfigOptions],
    ) -> "QueueConfig[T]":
        ...

    @typing.overload
    def replace(self, **kwds: Unpack[QueueConfigOptions[M_co]]) -> Self:
        ...

    def replace(self, **kwds):  # type: ignore
        return dataclasses.replace(self, **kwds)
