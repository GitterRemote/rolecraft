import collections
from .role import Role


class RoleHanger(collections.UserList):
    def put(self, role: Role):
        self.append(role)
