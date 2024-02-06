import collections

from rolecraft.queue_factory import QueueAndNameKeys
from rolecraft.role_lib import RoleHanger, default_role_hanger
from rolecraft.service import QueueDiscovery

from . import config_store as _config_store


class DefaultQueueDiscovery(QueueDiscovery):
    """Discover queue names and/or paired broker from the Role and ConfigStore."""

    def __init__(
        self,
        role_hanger: RoleHanger | None = None,
    ) -> None:
        self.role_hanger = role_hanger or default_role_hanger

    def __call__(self) -> QueueAndNameKeys:
        queue_names_by_broker = collections.defaultdict(set[str])
        queue_names = set[str]()
        rv = QueueAndNameKeys(
            queue_names=queue_names,
            queue_names_by_broker=queue_names_by_broker,
        )

        # add from role
        for role in self.role_hanger or ():
            queue_name = role.options.get("queue_name", "default")
            if broker := role.options.get("broker"):
                queue_names_by_broker[broker].add(queue_name)
            else:
                queue_names.add(queue_name)

        # add from config
        if config_store := _config_store.global_config_store:
            for (
                broker,
                broker_queue_names,
            ) in config_store.parsed_queue_names_by_broker().items():
                queue_names_by_broker[broker].update(broker_queue_names)

        # add default
        if not queue_names_by_broker and not queue_names:
            queue_names.add("default")

        return rv
