from typing import Protocol
from rolecraft.broker import Broker
from rolecraft.role_lib import RoleHanger
from rolecraft.config import ConfigStore


class QueueDiscovery(Protocol):
    def __call__(self) -> dict[str, Broker | None]:
        """Returns: mapping from queue name to the broker

        No need to care about QueueConfigOptions bound to the Role. That's role's QueueConfigOptions instead of queue's QueueConfigOptions.
        """
        ...


class DefaultQueueDiscovery(QueueDiscovery):
    """
    - [x] Role bound queue names
    - [ ] Queue name from ConfigStore's specific queue configs
    - [ ] Queue name from ConfigurableConfig queue names by broker
    """

    def __init__(
        self,
        role_hanger: RoleHanger | None = None,
        config_store: ConfigStore | None = None,
    ) -> None:
        self.role_hanger = role_hanger
        self.config_store = config_store

    def __call__(self) -> dict[str, Broker | None]:
        queue_names = dict[str, Broker | None]()
        for role in self.role_hanger or ():
            if role.queue_name:
                broker = role.options.get("broker")
                queue_names[role.queue_name] = broker

        # TODO: implement ConfigStore get queue names
        return queue_names
