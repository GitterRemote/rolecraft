import dataclasses


@dataclasses.dataclass
class BytesRawMessage:
    id: str | bytes
    data: bytes
