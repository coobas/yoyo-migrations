import pytest

from yoyo.tests import get_test_backends


@pytest.yield_fixture(params=get_test_backends())
def backend_fixture(request):
    """
    Return all backends configured in ``test_databases.ini``
    """
    backend = request.param
    try:
        yield backend
    finally:
        backend.rollback()
        for table in (request.param.list_tables()):
            if table.startswith('_yoyo'):
                with backend.transaction():
                    backend.execute("DROP TABLE {}".format(table))
