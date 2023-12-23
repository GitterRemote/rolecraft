from __future__ import annotations

import collections
import typing

if typing.TYPE_CHECKING:
    from .role import Role


class RoleHanger(collections.UserList):
    def put(self, role: Role):
        self.append(role)

    def pick(self, role_name: str) -> Role | None:
        pass
