import pytest

from yoyo.tests import get_test_backends


@pytest.yield_fixture(params=get_test_backends())
def backend_fixture(request):
    """
    Return all backends configured in ``test_databases.ini``
    """
    yield request.param
