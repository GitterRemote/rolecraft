import threading


class Local:
    def __init__(self, local: threading.local) -> None:
        self._thread_local = local

    @property
    def stop_event(self) -> threading.Event | None:
        return getattr(self._thread_local, "stop_event", None)

    @stop_event.setter
    def stop_event(self, value):
        self._thread_local.stop_event = value


local = Local(threading.local())
