import dataclasses
from . import queue


@dataclasses.dataclass
class Meta:
    retries: int | None = None


@dataclasses.dataclass
class Message:
    id: str
    queue: "queue.MessageQueue"
    meta: Meta

    role_name: str
    args: tuple  # Discussion: if Message doesn't have role access, then args and kwargs cannot be decode properly. However, Message doesn't know how to map the role name to the role itself, neither does Encoder. It is worker's responsibliity to find the correct role by the name in the message.
    kwargs: dict

    # stub following metheods for convenient
    def ack(self, **kwargs):
        return self.queue.ack(self, **kwargs)

    def nack(self, **kwargs):
        return self.queue.nack(self, **kwargs)

    def requeue(self, **kwargs):
        return self.queue.requeue(self, **kwargs)
