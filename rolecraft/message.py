import dataclasses
from . import queue


@dataclasses.dataclass
class Meta:
    retries: int | None = None


@dataclasses.dataclass
class Message:
    id: str
    data: bytes
    queue: "queue.Queue"
    meta: Meta

    # stub following metheods for convenient
    def ack(self, **kwargs):
        return self.queue.ack(self, **kwargs)

    def nack(self, **kwargs):
        return self.queue.nack(self, **kwargs)

    def requeue(self, **kwargs):
        return self.queue.requeue(self, **kwargs)
