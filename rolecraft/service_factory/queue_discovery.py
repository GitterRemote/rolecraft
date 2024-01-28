from typing import Protocol

from rolecraft.queue_factory import QueueAndNameKeys


class QueueDiscovery(Protocol):
    def __call__(self) -> QueueAndNameKeys:
        """Returns: mapping from queue name to the broker

        No need to care about QueueConfigOptions bound to the Role. That's role's QueueConfigOptions instead of queue's QueueConfigOptions.
        """
        ...
