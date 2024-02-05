import threading
from rolecraft.service import thread_local as local_mod


def test_in_different_threads():
    local = local_mod.thread_local
    rv = []

    def run():
        rv.append(local.stop_event is None)
        local.stop_event = threading.Event()

    e = threading.Event()
    local.stop_event = e

    t = threading.Thread(target=run)
    t.start()
    t.join()

    assert rv and all(rv)
    assert local.stop_event
