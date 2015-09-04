import pytest

from yoyo import backends
from yoyo import exceptions
from yoyo.tests import get_test_backends


class TestTransactionHandling(object):

    @pytest.yield_fixture(autouse=True, params=get_test_backends())
    def backend(self, request):
        backend = request.param
        with backend.transaction():
            if backend.__class__ is backends.MySQLBackend:
                backend.execute("CREATE TABLE _yoyo_t "
                                "(id CHAR(1) primary key) "
                                "ENGINE=InnoDB")
            else:
                backend.execute("CREATE TABLE _yoyo_t "
                                "(id CHAR(1) primary key)")
        yield backend
        with backend.transaction():
            backend.execute("DROP TABLE _yoyo_t")

    def test_it_commits(self, backend):
        with backend.transaction():
            backend.execute("INSERT INTO _yoyo_t values ('A')")

        with backend.transaction():
            rows = list(backend.execute("SELECT * FROM _yoyo_t").fetchall())
            assert rows == [('A',)]

    def test_it_rolls_back(self, backend):
        try:
            with backend.transaction():
                backend.execute("INSERT INTO _yoyo_t values ('A')")
                # Invalid SQL to produce an error
                backend.execute("INSERT INTO nonexistant values ('A')")
        except tuple(exceptions.DatabaseErrors):
            pass

        with backend.transaction():
            rows = list(backend.execute("SELECT * FROM _yoyo_t").fetchall())
            assert rows == []

    def test_it_nests_transactions(self, backend):
        with backend.transaction():
            backend.execute("INSERT INTO _yoyo_t values ('A')")

            with backend.transaction() as trans:
                backend.execute("INSERT INTO _yoyo_t values ('B')")
                trans.rollback()

            with backend.transaction() as trans:
                backend.execute("INSERT INTO _yoyo_t values ('C')")

        with backend.transaction():
            rows = list(backend.execute("SELECT * FROM _yoyo_t").fetchall())
            assert rows == [('A',), ('C',)]

    def test_backend_detects_transactional_ddl(self, backend):
        expected = {backends.PostgresqlBackend: True,
                    backends.SQLiteBackend: True,
                    backends.MySQLBackend: False}
        if backend.__class__ in expected:
            assert backend.has_transactional_ddl is expected[backend.__class__]
