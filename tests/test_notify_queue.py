import pytest
import threading
from unittest import mock
import time
from rolecraft import notify_queue as notify_queue_mod


@pytest.fixture()
def notify_queue():
    return notify_queue_mod.NotifyQueue(maxsize=1)


def test_get_no_wait(notify_queue):
    assert not notify_queue.get_nowait()

    notify_queue.put(1)
    assert notify_queue.get_nowait() == 1
    assert not notify_queue.get_nowait()


def test_get(notify_queue):
    rv = []

    def consume():
        rv.append(notify_queue.get() == 1)
        rv.append(not notify_queue.get_nowait())
        rv.append(notify_queue.get() == 2)

    t = threading.Thread(target=consume)
    t.start()

    notify_queue.put(1)
    time.sleep(0.1)
    notify_queue.put(2)
    t.join()

    assert len(rv) == 3 and all(rv)

    notify_queue.put(3)
    assert notify_queue.get() == 3


def test_notify_all(notify_queue):
    rv = []

    def consume():
        rv.append(notify_queue.get())

    threads = []
    for i in range(3):
        t = threading.Thread(target=consume)
        t.start()
        threads.append(t)

    time.sleep(0.1)
    assert not rv

    with notify_queue._condition:
        notify_queue._condition.notify()
    time.sleep(0.1)
    assert rv == [None]

    notify_queue.notify_all()
    for t in threads:
        t.join()
    assert rv == [None, None, None]


def test_wakeup_until_notify_all(notify_queue):
    rv = []

    def consume():
        rv.append(notify_queue.get(wakeup_until_notify_all=True))

    threads = []
    for i in range(3):
        t = threading.Thread(target=consume)
        t.start()
        threads.append(t)

    time.sleep(0.1)
    assert not rv

    with notify_queue._condition:
        notify_queue._condition.notify()
    time.sleep(0.1)
    assert rv == []

    notify_queue.notify_all()
    for t in threads:
        t.join()
    assert rv == [None, None, None]


def test_put_before_wait(notify_queue):
    called = False

    wait = notify_queue._condition.wait

    def put_before_wait():
        nonlocal called
        if called:
            return wait()
        called = True
        t = threading.Thread(target=lambda: notify_queue.put(1))
        t.start()
        return wait()

    with mock.patch.object(
        notify_queue._condition, "wait", side_effect=put_before_wait
    ):
        assert notify_queue.get() == 1
