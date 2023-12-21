from typing import Callable
from .config import Config
from rolecraft.broker import Broker
from rolecraft.queue import Queue

ConfigFetcher = Callable[[Queue], Config]


class DefaultConfigFetcher:
    def __init__(
        self,
        config: Config | None = None,
        queue_configs: dict[str, Config] | None = None,
        broker_configs: dict[Broker, Config] | None = None,
    ):
        self.config = config
        self.queue_configs = queue_configs
        self.broker_configs = broker_configs

    def config_for(self, queue: Queue) -> Config:
        # FIXME: merge instead of ta
        return (
            self.queue_configs.get(queue.name)
            or self.broker_configs.get(queue.broker)
            or self.config
        )

    __call__ = config_for
