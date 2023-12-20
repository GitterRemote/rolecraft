from collections.abc import Callable


class Role[**P, R]:
    def __init__(
        self,
        fn: Callable[P, R],
        name: str | None = None,
        *,
        queue_name="default",
        **options,
    ) -> None:
        self.fn = fn
        self._name = name
        self.queue_name = queue_name
        self.options = options

    @property
    def name(self) -> str:
        return self._name or self.fn.__name__

    def __call__(self, *args: P.args, **kwds: P.kwargs) -> R:
        return self.fn(*args, **kwds)

    # send method with broker
    # create message method
