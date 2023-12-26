import pytest

from rolecraft.role_lib import serializer as serializer_mod


@pytest.fixture
def str_serializer():
    return serializer_mod.str_serializer
