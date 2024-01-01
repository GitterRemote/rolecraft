from typing import Any

from . import config_store as _config_store
from . import queue_config as _queue_config
from .config_store import ConfigFetcher, ConfigStore


@ConfigStore.register
class _Proxy:
    def __init__(self, global_default: "GlobalDefault") -> None:
        self._global_default = global_default

    @property
    def _config_store(self) -> ConfigStore:
        return self._global_default.get_or_default()

    def __getattr__(self, name: str):
        return getattr(self._config_store, name)

    class _FetcherProxy:
        def __init__(self, proxy: "_Proxy") -> None:
            self._proxy = proxy

        def __getattr__(self, name: str):
            return getattr(self._proxy._config_store.fetcher, name)

        def __call__(self, *args, **kwargs) -> Any:
            return self._proxy._config_store.fetcher(*args, **kwargs)

    @property
    def fetcher(self) -> ConfigFetcher:
        return self._FetcherProxy(self)  # type: ignore


class GlobalDefault:
    def __init__(self) -> None:
        self.default: ConfigStore | None = None

    def set(self, config_store: ConfigStore):
        """set the store to the global variable as a default store"""
        self.default = config_store

    def get(self) -> ConfigStore | None:
        return self.default

    def get_or_default(self) -> ConfigStore:
        return self.get() or _config_store.SimpleConfigStore(
            queue_config=_queue_config.IncompleteQueueConfig.default()
        )

    def get_future(self) -> ConfigStore:
        return _Proxy(self)  # type: ignore

    def get_or_future(self) -> ConfigStore:
        return self.get() or self.get_future()


global_default = GlobalDefault()
