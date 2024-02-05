import functools

import rolecraft.thread_local as _thread_local
from rolecraft import config as _config
from rolecraft import role_lib as _role_lib
from rolecraft import service_factory as _service_factory
from rolecraft.broker import Broker
from rolecraft.config import ConfigStore, get_config_fetcher
from rolecraft.config import ConfigurableConfig as Config
from rolecraft.role_lib import ActionError, InterruptError
from rolecraft.service_factory import ServiceCreateOptions
from rolecraft.thread_local import StopEvent

RoleDecorator = functools.partial(
    _role_lib.RoleDecorator, config_fetcher=get_config_fetcher()
)
ServiceFactory = functools.partial(
    _service_factory.ServiceFactory,
    config_fetcher=get_config_fetcher(),
    queue_discovery=_config.DefaultQueueDiscovery(),
)
role = RoleDecorator()
local = _thread_local.thread_local

__all__ = [
    "Broker",
    "RoleDecorator",
    "role",
    "ServiceFactory",
    "ConfigStore",
    "Config",
    "local",
    "ServiceCreateOptions",
    "InterruptError",
    "StopEvent",
    "ActionError",
]
