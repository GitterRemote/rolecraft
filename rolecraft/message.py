from __future__ import annotations

import dataclasses
import typing

if typing.TYPE_CHECKING:
    from .queue import MessageQueue


@dataclasses.dataclass
class Meta:
    """All fields should be optional, and only be one of types int, float, str"""

    retries: int | None = None


@dataclasses.dataclass(kw_only=True)
class Message:
    id: str = ""
    meta: Meta = dataclasses.field(default_factory=Meta)

    role_name: str
    role_data: str | bytes | None = None

    queue: MessageQueue

    # Stub queue metheods for convenient
    def enqueue(self, **kwargs):
        return self.queue.enqueue(self, **kwargs)

    def ack(self, **kwargs):
        return self.queue.ack(self, **kwargs)

    def nack(self, **kwargs):
        return self.queue.nack(self, **kwargs)

    def requeue(self, **kwargs):
        return self.queue.requeue(self, **kwargs)
