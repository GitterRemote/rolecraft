import abc
import dataclasses
import json
import struct
import typing

from .broker import BytesRawMessage, HeaderBytesRawMessage
from .message import Message, Meta


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
        msg_dict = dataclasses.asdict(message)
        msg_dict.pop("id")
        msg_dict.pop("meta")
        msg_dict.pop("queue")

        data = json.dumps(msg_dict).encode()
        headers = self._encode_meta(message.meta)
        return HeaderBytesRawMessage(id=message.id, data=data, headers=headers)

    def _encode_meta(self, meta) -> dict[str, _META_VALUE_TYPE]:
        """excludes the None value from dict"""
        data = {}
        for field in dataclasses.fields(meta):
            value = getattr(meta, field.name)
            if value is not None:
                # assert type(value) in self._META_VALUE_TYPE
                data[field.name] = value
        return data

    def _decode_meta(self, data: dict[str, _META_VALUE_TYPE]) -> Meta:
        meta = Meta()
        for field in dataclasses.fields(Meta):
            value = data.get(field.name)
            if value is not None:
                setattr(
                    meta, field.name, self._convert_type(value, field.type)
                )
        return meta

    def _convert_type(self, value, field_type):
        if isinstance(field_type, str):
            if "int" in field_type:
                return int(value)
            elif "str" in field_type:
                return str(value)
            elif "float" in field_type:
                return float(field_type)
            else:
                raise RuntimeError(
                    f"Unknown field type {field_type} for {value}"
                )

        for t in typing.get_args(field_type):
            if type(None) is t:
                continue
            return t(value)
        else:
            raise RuntimeError(f"Unknown field type {field_type} for {value}")

    def decode(
        self, raw_message: HeaderBytesRawMessage, *, queue, **kwargs
    ) -> Message:
        msg_dict = json.loads(raw_message.data)
        msg_dict["id"] = raw_message.id
        msg_dict["queue"] = queue
        msg_dict["meta"] = self._decode_meta(raw_message.headers)
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
