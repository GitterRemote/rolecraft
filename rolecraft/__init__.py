from .broker import default_broker as _default_broker
from .broker import Broker


def set_broker(broker: Broker):
    global _default_broker
    _default_broker = broker


def get_broker() -> Broker | None:
    return _default_broker


__all__ = ["set_broker", "get_broker", "Broker"]
