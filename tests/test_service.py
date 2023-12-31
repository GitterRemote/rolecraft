import contextlib
import threading
import time

import pytest

import rolecraft
from rolecraft import broker as broker_mod
from rolecraft import role


@pytest.fixture
def broker():
    return broker_mod.StubBroker()


@pytest.fixture(autouse=True)
def config(broker):
    rolecraft.Config().set_default(broker=broker).inject()


@pytest.fixture
def create_service():
    @contextlib.contextmanager
    def _create_service(thread_num: int = 1):
        service = rolecraft.ServiceFactory().create()
        t = threading.Thread(
            target=service.start,
            kwargs=dict(thread_num=thread_num, ignore_signal=True),
        )
        t.start()
        yield service
        service.stop()
        service.join()
        t.join()

    return _create_service


def test_dispatch_messages(create_service):
    rv = []

    # 1, 3, 6, 10
    @role
    def fn(first: int, *, second: int):
        rv.append(first + second)

    with create_service():
        fn.dispatch_message(0, second=1)
        time.sleep(0.1)
    assert len(rv) == 1
    assert rv[0] == 1

    rv.clear()
    with create_service():
        for i in range(100):
            fn.dispatch_message(i, second=i)
        time.sleep(0.1)
    assert len(rv) == 100
    assert rv[0] == 0
    assert rv[-1] == 99 * 2
