import dataclasses
from typing import TypedDict, TypeVar

from rolecraft.broker import Broker
from rolecraft.encoder import Encoder
from rolecraft.middleware import Middleware, MiddlewareList

M_co = TypeVar("M_co", covariant=True)


class QueueConfigKeys(TypedDict, total=False):
    middlewares: list[Middleware] | MiddlewareList | None
    consumer_wait_time_seconds: int | None


@dataclasses.dataclass(kw_only=True, frozen=True)
class _QueueConfig:
    middlewares: list[Middleware] | MiddlewareList | None = None
    consumer_wait_time_seconds: int | None = None


@dataclasses.dataclass(kw_only=True, frozen=True)
class QueueConfig[M_co](_QueueConfig):
    encoder: Encoder[M_co]
    broker: Broker[M_co]


@dataclasses.dataclass(kw_only=True, frozen=True)
class IncompleteQueueConfig[M_co](_QueueConfig):
    encoder: Encoder[M_co]
    broker: Broker[M_co] | None = None

    def to_queue_config(self) -> QueueConfig[M_co]:
        assert self.broker
        return QueueConfig(
            **{f.name: getattr(self, f.name) for f in dataclasses.fields(self)}
        )
