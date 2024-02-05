import threading
from collections.abc import Callable
from typing import Any

from rolecraft.role_lib import InterruptError

__all__ = ["ThreadLocal", "thread_local", "InterruptError", "StopEvent"]


class ThreadLocal:
    def __init__(self, local: threading.local) -> None:
        self._thread_local = local

    @property
    def stop_event(self) -> "StopEvent | None":
        """A `threading.Event` that includes additional optional parameters in the wait method.

        You can utilize it to trigger an InterruptError and perform cleanup or wrap-up actions. For more details, please refer to the StopEvent class.

        You can also use it as a standard `threading.Event`.
        """
        return getattr(self._thread_local, "stop_event", None)

    @stop_event.setter
    def stop_event(self, value: threading.Event):
        if not isinstance(value, StopEvent):
            value = StopEvent(value)
        self._thread_local.stop_event = value

    @property
    def ensured_interrupt_event(self) -> "StopEvent":
        """A convenient property for StopEvent which has interrupt set to True"""
        ev = self.stop_event
        assert ev
        ev.interrupt = True
        return ev


thread_local = ThreadLocal(threading.local())


class StopEvent(threading.Event):
    def __init__(self, event: threading.Event) -> None:
        self._event = event
        self.interrupt: bool = False

    def wait(
        self,
        timeout: float | None = None,
        *,
        interrupt: bool | None = None,
        cleanup: Callable[[], Any] | None = None,
    ) -> bool:
        """Allow to raise InterruptError when the event is set, instead of returning True.

        You can catch the InterruptError, perform the necessary wrap-up or clean-up actions, and then re-raise the error. The worker will handle the InterruptError, for example, by requeuing the message.

        And you can use it as a normal `threading.Event` as well.

        Parameters:
            - timeout (float | None): Maximum time to wait for the event (in seconds). If None, the wait is indefinite.
            - interrupt (bool): If True, raises an InterruptError upon event set.
            - cleanup (callable | None): A callable function for cleanup or wrap-up before raising the InterruptError.

        Returns:
            bool: True if the event is set, False if the timeout occurs.

        Raises:
            InterruptError: If `interrupt` is True, and the event is set.
        """
        if self._event.wait(timeout):
            if interrupt is True or (interrupt is None and self.interrupt):
                if cleanup:
                    cleanup()
                raise InterruptError
            return True
        return False

    def check(self):
        """Check if the flag is set and raise an InterruptError if it is."""
        if self._event.is_set():
            raise InterruptError

    def __getattr__(self, name: str):
        return getattr(self._event, name)
