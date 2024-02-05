from __future__ import annotations

import abc
import collections
import typing
from collections.abc import Iterator
from typing import Any

if typing.TYPE_CHECKING:
    from . import Role


class DuplicatedRoleError(Exception):
    def __init__(self, role: Role, *args: object) -> None:
        self.role = role
        super().__init__(*args)


class RoleHanger(abc.ABC):
    @abc.abstractmethod
    def put(self, role: Role[Any, Any, Any]):
        raise NotImplementedError

    @abc.abstractmethod
    def pick(self, role_name: str) -> Role | None:
        raise NotImplementedError

    @abc.abstractmethod
    def __iter__(self) -> Iterator[Role]:
        raise NotImplementedError

    @abc.abstractmethod
    def clear(self):
        raise NotImplementedError


class SimpleRoleHanger(collections.UserDict[str, "Role"], RoleHanger):
    def put(self, role: Role[Any, Any, Any]):
        if role.name in self:
            raise DuplicatedRoleError(role=role)
        self[role.name] = role

    def pick(self, role_name: str) -> Role | None:
        return self.get(role_name)

    def __iter__(self) -> Iterator[Role]:
        return iter(self.data.values())

    def clear(self):
        self.data.clear()


default_role_hanger = SimpleRoleHanger()
