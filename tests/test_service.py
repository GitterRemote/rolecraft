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


@pytest.fixture(autouse=True)
def clear_role_hanger():
    yield
    rolecraft.default_role_hanger.clear()


@pytest.fixture(params=[1, 2, 3], ids=["1 worker", "2 workers", "3 workers"])
def create_service(request):
    thread_num = request.param

    @contextlib.contextmanager
    def _create_service(thread_num: int = thread_num):
        service = rolecraft.ServiceFactory().create(prefetch_size=1)
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
