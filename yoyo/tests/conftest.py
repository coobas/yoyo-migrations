import pytest

from yoyo.connections import get_backend
from yoyo.tests import get_test_dburis


@pytest.yield_fixture(params=get_test_dburis())
def backend_fixture(request):
    """
    Return all backends configured in ``test_databases.ini``
    """
    backend = get_backend(request.param)
    try:
        yield backend
    finally:
        backend.rollback()
        for table in (backend.list_tables()):
            if table.startswith('_yoyo'):
                with backend.transaction():
                    backend.execute("DROP TABLE {}".format(table))
