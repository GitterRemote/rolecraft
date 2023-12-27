from collections import defaultdict
from typing import Protocol

from rolecraft.config import ConfigStore
from rolecraft.queue_factory import QueueAndNameKeys
from rolecraft.role_lib import RoleHanger


class QueueDiscovery(Protocol):
    def __call__(
        self, config_store: ConfigStore | None = None
    ) -> QueueAndNameKeys:
        """Returns: mapping from queue name to the broker

        No need to care about QueueConfigOptions bound to the Role. That's role's QueueConfigOptions instead of queue's QueueConfigOptions.
        """
        ...


class DefaultQueueDiscovery(QueueDiscovery):
    """Discover queue names and/or paired broker from the Role and ConfigStore."""

    def __init__(
        self,
        role_hanger: RoleHanger | None = None,
        config_store: ConfigStore | None = None,
    ) -> None:
        self.role_hanger = role_hanger
        self.config_store = config_store

    def __call__(
        self, config_store: ConfigStore | None = None
    ) -> QueueAndNameKeys:
        queue_names_by_broker = defaultdict(list[str])
        queue_names: list[str] = []
        rv = QueueAndNameKeys(
            queue_names=queue_names,
            queue_names_by_broker=queue_names_by_broker,
        )

        for role in self.role_hanger or ():
            if not role.queue_name:
                continue
            broker = role.options.get("broker")
            if broker:
                queue_names_by_broker[broker].append(role.queue_name)
            else:
                queue_names.append(role.queue_name)

        config_store = config_store or self.config_store
        if config_store:
            queue_names_by_broker.update(
                config_store.parsed_queue_names_by_broker
            )
        return rv
