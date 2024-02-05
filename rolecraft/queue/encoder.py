import abc
import dataclasses
import json
import struct
from typing import Any

from rolecraft.broker import BytesRawMessage, HeaderBytesRawMessage

from .message import Message


class Encoder[RawMessage](abc.ABC):
    @abc.abstractmethod
    def encode(self, message: Message) -> RawMessage:
        raise NotImplementedError

    @abc.abstractmethod
    def decode(self, raw_message: RawMessage, **kwargs) -> Message:
        raise NotImplementedError


class HeaderBytesEncoder(Encoder[HeaderBytesRawMessage]):
    _META_VALUE_TYPE = str | int | float

    def encode(self, message: Message) -> HeaderBytesRawMessage:
        if isinstance(message.role_data, bytes):
            raise NotImplementedError
        msg_dict = self._to_dict(message)

        data = json.dumps(msg_dict).encode()
        return HeaderBytesRawMessage(
            id=message.id, data=data, headers=message.meta
        )

    def _to_dict(self, message: Message) -> dict[str, Any]:
        data = {}
        for field in dataclasses.fields(message):
            if field.name in ("id", "meta", "queue"):
                continue
            data[field.name] = getattr(message, field.name)
        return data

    def decode(
        self, raw_message: HeaderBytesRawMessage, *, queue, **kwargs
    ) -> Message:
        msg_dict = json.loads(raw_message.data)
        msg_dict["id"] = raw_message.id
        msg_dict["queue"] = queue
        msg_dict["meta"] = raw_message.headers
        return Message(**msg_dict)


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
