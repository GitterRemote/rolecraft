import dataclasses
from typing import Self


@dataclasses.dataclass(kw_only=True)
class RawMessage:
    id: str = ""


@dataclasses.dataclass(kw_only=True)
class BytesRawMessage(RawMessage):
    id: str = ""
    data: bytes


@dataclasses.dataclass(kw_only=True)
class HeaderBytesRawMessage(BytesRawMessage):
    id: str = ""
    data: bytes
    headers: dict[str, str | int | float] = dataclasses.field(
        default_factory=dict
    )

    def replace(self, **kwds) -> Self:
        return dataclasses.replace(self, **kwds)
