import threading

__all__ = ["ThreadLocal", "thread_local", "InterruptError", "InterruptEvent"]


class ThreadLocal:
    def __init__(self, local: threading.local) -> None:
        self._thread_local = local

    @property
    def stop_event(self) -> threading.Event | None:
        return getattr(self._thread_local, "stop_event", None)

    @stop_event.setter
    def stop_event(self, value):
        self._thread_local.stop_event = value

    @property
    def interrupt_event(self) -> "InterruptEvent | None":
        return InterruptEvent(self.stop_event) if self.stop_event else None


thread_local = ThreadLocal(threading.local())


class InterruptError(Exception):
    ...


class InterruptEvent(threading.Event):
    def __init__(self, event: threading.Event) -> None:
        self._event = event

    def wait(self, timeout: float | None = None) -> bool:
        """Raises InterruptError when the event is set, instead of returning True.

        You can catch the InterruptError, perform the necessary wrap-up or clean-up actions, and then re-raise the error. The worker will handle the InterruptError, for example, by requeuing the message.
        """
        if self._event.wait(timeout):
            raise InterruptError
        return False

    def __getattr__(self, name: str):
        return getattr(self._event, name)
