class CraftError(Exception):
    ...


class UnmatchedQueueNameError(CraftError):
    ...


class SerializeError(CraftError):
    ...


class DeserializeError(CraftError):
    ...


class ActionError(CraftError):
    ...
