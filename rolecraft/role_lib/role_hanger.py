from __future__ import annotations

import collections
from collections.abc import Iterator
import typing
from typing import Any

if typing.TYPE_CHECKING:
    from . import Role


class DuplicatedRoleError(Exception):
    def __init__(self, role: Role, *args: object) -> None:
        self.role = role
        super().__init__(*args)


class RoleHanger(collections.UserDict[str, "Role"]):
    def put(self, role: Role[Any, Any, Any]):
        if role.name in self:
            raise DuplicatedRoleError(role=role)
        self[role.name] = role

    def pick(self, role_name: str) -> Role | None:
        return self.get(role_name)

    def __iter__(self) -> Iterator[Role]:
        return iter(self.data.values())


default_role_hanger = RoleHanger()
