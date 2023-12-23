import dataclasses


@dataclasses.dataclass
class BytesRawMessage:
    id: str
    data: bytes


@dataclasses.dataclass
class HeaderBytesRawMessage:
    id: str
    data: bytes
    headers: dict[str, str]
