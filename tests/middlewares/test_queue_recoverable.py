import pytest

from rolecraft.broker import RecoverableError
from rolecraft.message import Message
from rolecraft.middlewares import queue_recoverable as queue_recoverable_mod


@pytest.fixture()
def middleware(queue):
    q = queue_recoverable_mod.QueueRecoverable(queue)
    q.is_outmost = True
    return q


@pytest.fixture()
def new_message(queue):
    def _new_message(role_name: str = "test_role_name"):
        return Message(role_name=role_name, queue=queue)

    return _new_message


def test_recoverable_error_for_receive(queue, middleware, new_message):
    msgs = [new_message(), new_message()]
    queue.receive.side_effect = [RecoverableError, msgs]
    assert callable(queue.receive)

    rv = middleware.receive()
    assert rv == msgs
    assert queue.receive.call_count == 2


def test_recoverable_error_for_ack():
    ...


def test_irrecoverable_error():
    ...


def test_recoverable_error_exceeds_max_retries():
    ...
