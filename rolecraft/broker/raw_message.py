import dataclasses


@dataclasses.dataclass(kw_only=True)
class BytesRawMessage:
    id: str = ""
    data: bytes


@dataclasses.dataclass(kw_only=True)
class HeaderBytesRawMessage(BytesRawMessage):
    id: str = ""
    data: bytes
    headers: dict[str, str | int | float] = dataclasses.field(
        default_factory=dict
    )
