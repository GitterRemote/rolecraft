import abc
import json
import struct

from .message import Message
from .broker import BytesRawMessage, HeaderBytesRawMessage


class Encoder[RawMessage](abc.ABC):
    @abc.abstractmethod
    def encode(self, message: Message) -> RawMessage:
        raise NotImplementedError

    def decode(self, raw_message: RawMessage, **kwargs) -> Message:
        raise NotImplementedError


class HeaderBytesEncoder(Encoder[HeaderBytesRawMessage]):
    pass


class BytesEncoder(Encoder[BytesRawMessage]):
    def __init__(self, encoder: HeaderBytesEncoder) -> None:
        self.encoder = encoder

    def encode(self, message: Message) -> BytesRawMessage:
        encoded = self.encoder.encode(message)
        header_data = json.dumps(encoded.headers).encode()
        data = self._pack(header_data, encoded.data)
        return BytesRawMessage(id=encoded.id, data=data)

    def _pack(self, header_data: bytes, data: bytes):
        header = struct.pack("!H", len(header_data))
        return header + header_data + data

    def _unpack(self, packed_data: bytes):
        header_data_len, _ = struct.unpack("!H", packed_data[:2])
        header_data = packed_data[2 : 2 + header_data_len]
        data = packed_data[2 + header_data_len :]
        return header_data, data

    def decode(self, raw_message: BytesRawMessage, **kwargs) -> Message:
        header_data, data = self._unpack(raw_message.data)
        headers = json.loads(header_data)
        msg = HeaderBytesRawMessage(
            id=raw_message.id,
            data=data,
            headers=headers,
        )
        return self.encoder.decode(msg, **kwargs)
