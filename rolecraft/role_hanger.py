from __future__ import annotations

import collections
import typing

if typing.TYPE_CHECKING:
    from .role_lib import Role


class DuplicatedRoleError(Exception):
    def __init__(self, role: Role, *args: object) -> None:
        self.role = role
        super().__init__(*args)


class RoleHanger(collections.UserDict[str, "Role"]):
    def put(self, role: Role):
        if role.name in self:
            raise DuplicatedRoleError(role=role)
        self[role.name] = role

    def pick(self, role_name: str) -> Role | None:
        return self.get(role_name)


default_role_hanger = RoleHanger()
