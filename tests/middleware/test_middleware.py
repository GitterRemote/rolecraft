from unittest import mock

import pytest

from rolecraft import middleware as middleware_mod
from rolecraft import queue as queue_mod
from rolecraft.queue import MessageQueue


@pytest.fixture()
def queue():
    q = mock.MagicMock(queue_mod.MessageQueue)
    q.name = "MockedQueue"
    return q


@pytest.fixture()
def middleware():
    class M(middleware_mod.Middleware):
        def __init__(
            self,
            queue: MessageQueue | None = None,
            *,
            a: int = 1,
            b: str = "2",
        ) -> None:
            super().__init__(queue)
            self.a = a
            self.b = b

        @property
        def options(self):
            return {"a": self.a, "b": self.b}

    return M()


def test_middleware_call(middleware, queue):
    m = middleware(queue)
    assert m is not middleware
    assert isinstance(m, MessageQueue)
    assert m.queue is queue
    assert middleware(queue) is not middleware(queue)
    assert m.a == middleware.a
    assert m.b == middleware.b

    middleware.b = "3"
    m2 = middleware(queue)
    assert m2.b == "3"


def test_middleware_delegate(middleware, queue):
    m = middleware(queue)
    msg = mock.MagicMock()
    m.enqueue(msg)
    assert queue.enqueue.call_count == 1
    queue.enqueue.assert_called_once_with(msg)
