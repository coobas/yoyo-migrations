from mock import Mock
from mock import call
from mock import patch
import pytest

from yoyo import backends
from yoyo.tests import get_test_backends
from yoyo.tests import with_migrations


class TestTransactionHandling(object):

    @pytest.fixture(autouse=True, params=get_test_backends())
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
        backend.rollback()
        for table in (backend.list_tables()):
            if table.startswith('_yoyo'):
                with backend.transaction():
                    backend.execute("DROP TABLE {}".format(table))

    def test_it_commits(self, backend):
        with backend.transaction():
            backend.execute("INSERT INTO _yoyo_t values ('A')")

        with backend.transaction():
            rows = list(backend.execute("SELECT * FROM _yoyo_t").fetchall())
            assert rows == [('A',)]

    def test_it_rolls_back(self, backend):
        with pytest.raises(backend.DatabaseError):
            with backend.transaction():
                backend.execute("INSERT INTO _yoyo_t values ('A')")
                # Invalid SQL to produce an error
                backend.execute("INSERT INTO nonexistant values ('A')")

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

    def test_non_transactional_ddl_behaviour(self, backend):
        """
        DDL queries in MySQL commit the current transaction,
        but it still seems to respect a subsequent rollback.

        We don't rely on this behaviour, but it's weird and worth having
        a test to document how it works and flag up in future should a new
        backend do things differently
        """
        if backend.has_transactional_ddl:
            return

        with backend.transaction() as trans:
            backend.execute("CREATE TABLE _yoyo_a (id INT)")  # implicit commit
            backend.execute("INSERT INTO _yoyo_a VALUES (1)")
            backend.execute("CREATE TABLE _yoyo_b (id INT)")  # implicit commit
            backend.execute("INSERT INTO _yoyo_b VALUES (1)")
            trans.rollback()

        count_a = backend.execute("SELECT COUNT(1) FROM _yoyo_a")\
                .fetchall()[0][0]
        assert count_a == 1

        count_b = backend.execute("SELECT COUNT(1) FROM _yoyo_b")\
                .fetchall()[0][0]
        assert count_b == 0

    @with_migrations(a="""
        __transactional__ = False
        step('CREATE DATABASE yoyo_test_tmp',
             'DROP DATABASE yoyo_test_tmp',
             )
    """)
    def test_statements_requiring_no_transaction(self, tmpdir):
        """
        PostgreSQL will error if certain statements (eg CREATE DATABASE)
        are run within a transaction block.

        As far as I know this behavior is PostgreSQL specific. We can't run
        this test in sqlite as it does not support CREATE DATABASE.
        """
        from yoyo import read_migrations
        for backend in get_test_backends(exclude={'sqlite'}):
            migrations = read_migrations(tmpdir)
            backend.apply_migrations(migrations)
            backend.rollback_migrations(migrations)


class TestInitConnection(object):

    class MockBackend(backends.DatabaseBackend):
        driver = Mock(DatabaseError=Exception, paramstyle='format')

        def connect(self, dburi):
            return Mock()

    def test_it_calls_init_connection(self):

        with patch.object(self.MockBackend, 'init_connection', Mock()) as mock_init:

            backend = self.MockBackend('', '')
            connection = backend.connection
            assert mock_init.call_args == call(connection)

            mock_init.reset_mock()
            backend.rollback()
            assert mock_init.call_args_list == [call(connection)]

    def test_postgresql_backend_sets_search_path(self):
        class MockPGBackend(backends.PostgresqlBackend):
            driver = Mock(DatabaseError=Exception, paramstyle='format')
            schema = 'foo'

            def connect(self, dburi):
                return Mock()

        backend = MockPGBackend('', '')
        backend.rollback()
        assert backend.connection.cursor().execute.call_args == \
                call('SET search_path TO foo')
