from unittest import mock

import pytest

from rolecraft import queue as queue_mod


@pytest.fixture()
def queue():
    q = mock.MagicMock(queue_mod.MessageQueue)
    q.name = "MockedQueue"
    return q
