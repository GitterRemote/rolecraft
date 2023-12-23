from typing import Callable
from .config import Config
from rolecraft.broker import Broker

ConfigFetcher = Callable[[str, Broker | None], Config]


class DefaultConfigFetcher:
    def __init__(
        self,
        config: Config | None = None,
        queue_configs: dict[str, Config] | None = None,
        broker_configs: dict[Broker, Config] | None = None,
    ):
        self.config = config
        self.queue_configs = queue_configs or {}
        self.broker_configs = broker_configs or {}

    # TODO: test config_for
    def config_for(
        self,
        queue_name: str,
        broker: Broker | None = None,
    ) -> Config:
        c = Config()
        for config in (
            self.queue_configs.get(queue_name),
            (broker and self.broker_configs.get(broker)),
            self.config,
            Config.default(),
        ):
            if config:
                c.merge_from(config)
        return c

    __call__ = config_for
