import abc
from abc import abstractmethod
from collections.abc import Callable, Hashable


class ReceiveFuture[R](abc.ABC, Hashable):
    @abstractmethod
    def result(self) -> R:
        """Return the result asap when cancel() is called."""
        raise NotImplementedError

    @abstractmethod
    def cancel(self):
        raise NotImplementedError

    def transform[T](
        self, transformer: Callable[[R], T]
    ) -> "ReceiveFuture[T]":
        return TransformerReceiveFuture(self, transformer)


class TransformerReceiveFuture[R, O](ReceiveFuture[O]):
    def __init__(
        self, future: ReceiveFuture[R], transformer: Callable[[R], O]
    ) -> None:
        self.future = future
        self.transformer = transformer

    def result(self) -> O:
        return self.transformer(self.future.result())

    def cancel(self):
        return self.future.cancel()

    def __hash__(self) -> int:
        return hash(self.future)
