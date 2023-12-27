import dataclasses
from typing import TypedDict, TypeVar

from rolecraft.broker import Broker
from rolecraft.encoder import Encoder
from rolecraft.middleware import Middleware, MiddlewareList

M_co = TypeVar("M_co", covariant=True)


class PartialQueueConfigOptions(TypedDict, total=False):
    middlewares: list[Middleware] | MiddlewareList
    consumer_wait_time_seconds: int | None


class QueueConfigOptions[M](PartialQueueConfigOptions, total=False):
    encoder: Encoder[M]
    broker: Broker[M]


@dataclasses.dataclass(kw_only=True, frozen=True)
class PartialQueueConfig:
    middlewares: list[Middleware] | MiddlewareList = dataclasses.field(
        default_factory=MiddlewareList
    )
    consumer_wait_time_seconds: int | None = None


@dataclasses.dataclass(kw_only=True, frozen=True)
class QueueConfig[M_co](PartialQueueConfig):
    encoder: Encoder[M_co]
    broker: Broker[M_co]
