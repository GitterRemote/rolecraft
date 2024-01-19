import dataclasses
import functools
import typing
from collections.abc import Iterable, MutableSequence, Sequence

from .middleware import Middleware
from .retryable import Retryable

M = Middleware


@dataclasses.dataclass(init=False, eq=True, order=False, repr=False)
class MiddlewareList(MutableSequence[M]):
    """A utility for configuring middlewares. If need to inherit this class to add new middlewares as attibutes, you have to add the decorator `@dataclasses.dataclass(init=False)` to the child class and overwrite the `field_name_for` method."""

    _middlewares: list[M]
    retryable: Retryable | None = None

    def __init__(self, middlewares: Sequence[M] = ()):
        self._set_field("_middlewares", [])

        for middleware in middlewares:
            self._append_middleware(middleware)

    def _append_middleware(self, middleware: M):
        self._middlewares.append(middleware)
        if field_name := self.name_for(middleware):
            self._set_field(field_name, middleware)

    def _remove_middleware(
        self, middleware: M, name: str | None = None, index: int | None = None
    ):
        if field_name := name or self.name_for(middleware):
            self._set_field(field_name, None)

        if index is not None:
            popped = self._middlewares.pop(index)
            assert popped is middleware
        else:
            self._middlewares.remove(middleware)

    def name_for(self, middleware: M) -> str | None:
        if isinstance(middleware, Retryable):
            return "retryable"
        return None

    def _remove_middleware_by_name(self, name: str):
        middleware = getattr(self, name)
        assert middleware, f"middleware doesn't exist for {name}"
        self._remove_middleware(middleware, name)

    def _set_field(self, name: str, value):
        super().__setattr__(name, value)

    @functools.cached_property
    def _field_names(self) -> set[str]:
        return set((field.name for field in dataclasses.fields(self)))

    def __setattr__(self, name, value):
        if name == "_middlewares":
            raise ValueError("_middlewares update is not allowed.")

        if name in self._field_names:
            if not getattr(self, name):
                raise ValueError(
                    "Update non-existing middleware is not allowed. "
                    "Please use append or insert method."
                )
            if value is None:
                self._remove_middleware_by_name(name)
                return
            elif self.name_for(value) != name:
                raise TypeError(f"Middleware type error for {name}: {value}")

        super().__setattr__(name, value)

    # -- MutableSequence implmenetation --
    def insert(self, index: int, value: M) -> None:
        if field_name := self.name_for(value):
            if getattr(self, field_name):
                raise ValueError(f"Middleware {field_name} exists")
            self._set_field(field_name, value)
        return self._middlewares.insert(index, value)

    def __setitem__(self, index: int, value: M) -> None:
        """Allow to override the middlewares with the same name or add the middleware that isn't in the list."""
        old_field_name = self.name_for(self._middlewares[index])
        if field_name := self.name_for(value):
            if old_field_name != field_name:
                if getattr(self, field_name):
                    raise ValueError(f"Middleware {field_name} exists")
                if old_field_name:
                    self._set_field(old_field_name, None)
            self._set_field(field_name, value)
        self._middlewares[index] = value

    def __delitem__(self, index: int) -> None:
        middleware = self._middlewares[index]
        self._remove_middleware(middleware, index=index)

    @typing.overload
    def __getitem__(self, index: int) -> M:
        ...

    @typing.overload
    def __getitem__(self, index: slice) -> typing.Self:
        ...

    def __getitem__(self, index: int | slice) -> M | typing.Self:
        if isinstance(index, slice):
            return self.__class__(self._middlewares[index])
        return self._middlewares[index]

    def __len__(self) -> int:
        return len(self._middlewares)

    # extra methods
    def __add__(self, values: Iterable[M]):
        return self.__class__(self._middlewares + list(values))

    def __radd__(self, values: Iterable[M]):
        return self.__class__(list(values) + self._middlewares)
