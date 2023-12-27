import dataclasses
from typing import Self, TypeVar

from rolecraft import encoder as _encoder
from rolecraft import middleware as _middleware
from rolecraft.broker import Broker
from rolecraft.encoder import Encoder
from rolecraft.queue_config import PartialQueueConfigOptions, QueueConfig

PartialQueueConfigOptions = PartialQueueConfigOptions

M_co = TypeVar("M_co", covariant=True)


@dataclasses.dataclass(kw_only=True, frozen=True)
class IncompleteQueueConfig[M_co](QueueConfig):
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
            consumer_wait_time_seconds=10 * 60,
        )
