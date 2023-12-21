import pytest
import threading
import time
from rolecraft import worker_pool as worker_pool_mod
from rolecraft import local as local_mod


@pytest.fixture()
def thread_worker_pool():
    pool = worker_pool_mod.ThreadWorkerPool(thread_num=2)
    pool.start()
    yield pool
    pool.stop()
    pool.join()


@pytest.fixture()
def single_thread_worker_pool():
    pool = worker_pool_mod.ThreadWorkerPool(thread_num=1)
    pool.start()
    yield pool
    pool.stop()
    pool.join()


@pytest.fixture()
def worker_pool(thread_worker_pool):
    # at least two worker threads
    yield thread_worker_pool


def test_submit(worker_pool):
    data = set()

    def put(i):
        data.add(i)

    worker_pool.submit(put, 1)
    worker_pool.submit(put, 2)

    with pytest.raises(RuntimeError):
        worker_pool.submit(3)

    worker_pool.join()

    assert len(data) == 2
    assert data == set((1, 2))


def test_thread_local_stop_event(worker_pool):
    data = set()

    def put(i):
        event = local_mod.local.stop_event
        event.wait(3600)
        data.add(i)

    worker_pool.submit(put, 1)
    worker_pool.submit(put, 2)

    time.sleep(0.1)
    assert not data

    worker_pool.stop()
    worker_pool.join()

    assert len(data) == 2
    assert data == set((1, 2))


def test_single_thread_worker_pool(single_thread_worker_pool):
    worker_pool = single_thread_worker_pool

    data = set()

    def put(i):
        event = local_mod.local.stop_event
        event.wait(3600)
        data.add(i)

    def run():
        worker_pool.submit(put, 1)

    t = threading.Thread(target=run)
    t.start()
    time.sleep(0.1)
    assert not data

    worker_pool.stop()
    worker_pool.join()
    # worker_pool.join doesn't work for the single thread case, we need to join
    # the thread here.
    t.join()

    assert data
