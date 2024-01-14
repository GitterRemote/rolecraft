class BrokerError(Exception):
    ...


class RecoverableError(Exception):
    ...


class IrrecoverableError(Exception):
    ...


class MessageNotFound(IrrecoverableError):
    ...


class QueueNotFound(IrrecoverableError):
    """if auto create queue is disabled for the broker, then it is possible to raise this error."""

    ...
