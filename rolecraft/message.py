from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .queue import MessageQueue


@dataclasses.dataclass
class Meta:
    """All fields should be optional, and only be one of types int, float, str"""

    retries: int | None = None


@dataclasses.dataclass
class Message:
    id: str
    meta: Meta

    role_name: str
    role_data: str | bytes | None

    queue: MessageQueue

    # stub following metheods for convenient
    def ack(self, **kwargs):
        return self.queue.ack(self, **kwargs)

    def nack(self, **kwargs):
        return self.queue.nack(self, **kwargs)

    def requeue(self, **kwargs):
        return self.queue.requeue(self, **kwargs)
