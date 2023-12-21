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
        self.queue_configs = queue_configs
        self.broker_configs = broker_configs

    def config_for(
        self,
        queue_name: str,
        broker: Broker | None = None,
    ) -> Config:
        # FIXME: merge instead of ta
        return (
            self.queue_configs.get(queue_name)
            or (broker and self.broker_configs.get(broker))
            or self.config
            or Config.default()
        )

    __call__ = config_for
