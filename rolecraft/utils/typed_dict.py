import typing
from typing import TypedDict


def subset_dict[T: TypedDict](parent: T, child_type: type[T]) -> T:
    return typing.cast(
        T,
        {
            key_name: parent.pop(key_name)  # type: ignore
            for key_name in child_type.__annotations__.keys()
            if key_name in parent
        },
    )
