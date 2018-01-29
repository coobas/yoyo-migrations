import pytest

from yoyo import backends
from yoyo.connections import get_backend
from yoyo.tests import get_test_dburis


@pytest.fixture(params=get_test_dburis())
def backend(request):
    """
    Return all backends configured in ``test_databases.ini``
    """
    backend = get_backend(request.param)
    with backend.transaction():
        if backend.__class__ is backends.MySQLBackend:
            backend.execute('CREATE TABLE _yoyo_t '
                            '(id CHAR(1) primary key) '
                            'ENGINE=InnoDB')
        else:
            backend.execute('CREATE TABLE _yoyo_t '
                            '(id CHAR(1) primary key)')
    try:
        yield backend
    finally:
        backend.rollback()
        for table in backend.list_tables():
            if table.startswith('_yoyo'):
                with backend.transaction():
                    backend.execute('DROP TABLE {}'.format(table))
