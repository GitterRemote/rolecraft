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


class InterruptError(ActionError):
    """Utilize InterruptError when the service is halted to signal the role action (function) to cease execution. This is particularly applicable in long-running user functions. Avoid using it in other scenarios."""

    ...
