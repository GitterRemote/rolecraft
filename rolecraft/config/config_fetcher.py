from rolecraft.queue_factory import ConfigFetcher

from . import config_store as _config_store
from . import default_queue_config as _default_queue_config


class ConfigFetcherProxy(ConfigFetcher):
    def __init__(self, default_fetcher: ConfigFetcher) -> None:
        self._default_fetcher = default_fetcher

    @property
    def _config_fetcher(self):
        if _config_store.global_config_store:
            return _config_store.global_config_store.fetcher
        return self._default_fetcher

    def __call__(self, *args, **kwds):
        return self._config_fetcher(*args, **kwds)


def get_config_fetcher() -> ConfigFetcher:
    if _config_store.global_config_store:
        return _config_store.global_config_store.fetcher
    return ConfigFetcherProxy(
        default_fetcher=_config_store.SimpleConfigStore(
            queue_config=_default_queue_config.DefaultQueueConfig()
        ).fetcher
    )
