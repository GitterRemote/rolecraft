import abc
from .message import Message
from .broker import BytesRawMessage


class Encoder[RawMessage](abc.ABC):
    @abc.abstractmethod
    def encode(self, message: Message) -> RawMessage:
        raise NotImplementedError

    def decode(self, raw_message: RawMessage) -> Message:
        raise NotImplementedError


class BytesEncoder(Encoder[BytesRawMessage]):
    def encode(self, message: Message) -> BytesRawMessage:
        raise NotImplementedError

    def decode(self, raw_message: BytesRawMessage) -> Message:
        raise NotImplementedError


class DefaultBytesEncoder(BytesEncoder):
    pass
