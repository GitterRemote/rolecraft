import dataclasses
import typing
from typing import Self, TypeVar, Unpack

from rolecraft import encoder as _encoder
from rolecraft import middleware as _middleware
from rolecraft.broker import Broker
from rolecraft.encoder import Encoder
from rolecraft.queue_config import (
    PartialQueueConfig,
    PartialQueueConfigOptions,
    QueueConfig,
)

M_co = TypeVar("M_co", covariant=True)


class IncompleteQueueConfigOptions[M](PartialQueueConfigOptions, total=False):
    encoder: Encoder[M]
    broker: Broker[M] | None


@dataclasses.dataclass(kw_only=True, frozen=True)
class IncompleteQueueConfig[M_co](PartialQueueConfig):
    encoder: Encoder[M_co]
    broker: Broker[M_co] | None = None

    def to_queue_config(self) -> QueueConfig[M_co]:
        assert self.broker
        return QueueConfig(
            **{f.name: getattr(self, f.name) for f in dataclasses.fields(self)}
        )

    @classmethod
    def default(cls) -> Self:
        return cls(
            middlewares=_middleware.MiddlewareList([_middleware.Retryable()]),
            encoder=_encoder.HeaderBytesEncoder(),
            wait_time_seconds=10 * 60,
        )

    @typing.overload
    def replace(self, **kwds: Unpack[PartialQueueConfigOptions]) -> Self:
        ...

    @typing.overload
    def replace[T](
        self,
        **kwds: Unpack[IncompleteQueueConfigOptions[T]],
    ) -> "IncompleteQueueConfig[T]":
        ...

    def replace(self, **kwds):  # type: ignore
        return dataclasses.replace(self, **kwds)
