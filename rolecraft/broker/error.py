class BrokerError(Exception):
    ...


class RecoverableError(Exception):
    ...


class IrrecoverableError(Exception):
    ...


class MessageNotFound(IrrecoverableError):
    ...
