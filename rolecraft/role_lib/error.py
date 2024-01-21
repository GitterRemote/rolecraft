class RoleError(Exception):
    ...


class CraftError(RoleError):
    ...


class DispatchError(RoleError):
    ...


class UnmatchedQueueNameError(CraftError):
    ...


class DeserializeError(CraftError):
    ...


class ActionError(CraftError):
    ...


class SerializeError(DispatchError):
    ...
