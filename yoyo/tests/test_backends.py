from mock import Mock
from mock import call
from mock import patch
import pytest
from threading import Thread
import time

from yoyo import backends
from yoyo import read_migrations
from yoyo import exceptions
from yoyo.connections import get_backend
from yoyo.tests import get_test_backends
from yoyo.tests import get_test_dburis
from yoyo.tests import with_migrations


class TestTransactionHandling(object):

    def test_it_commits(self, backend):
        with backend.transaction():
            backend.execute("INSERT INTO yoyo_t values ('A')")

        with backend.transaction():
            rows = list(backend.execute("SELECT * FROM yoyo_t").fetchall())
            assert rows == [('A',)]

    def test_it_rolls_back(self, backend):
        with pytest.raises(backend.DatabaseError):
            with backend.transaction():
                backend.execute("INSERT INTO yoyo_t values ('A')")
                # Invalid SQL to produce an error
                backend.execute("INSERT INTO nonexistant values ('A')")

        with backend.transaction():
            rows = list(backend.execute("SELECT * FROM yoyo_t").fetchall())
            assert rows == []

    def test_it_nests_transactions(self, backend):
        with backend.transaction():
            backend.execute("INSERT INTO yoyo_t values ('A')")

            with backend.transaction() as trans:
                backend.execute("INSERT INTO yoyo_t values ('B')")
                trans.rollback()

            with backend.transaction() as trans:
                backend.execute("INSERT INTO yoyo_t values ('C')")

        with backend.transaction():
            rows = list(backend.execute("SELECT * FROM yoyo_t").fetchall())
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
            backend.execute("CREATE TABLE yoyo_a (id INT)")  # implicit commit
            backend.execute("INSERT INTO yoyo_a VALUES (1)")
            backend.execute("CREATE TABLE yoyo_b (id INT)")  # implicit commit
            backend.execute("INSERT INTO yoyo_b VALUES (1)")
            trans.rollback()

        count_a = backend.execute("SELECT COUNT(1) FROM yoyo_a")\
                .fetchall()[0][0]
        assert count_a == 1

        count_b = backend.execute("SELECT COUNT(1) FROM yoyo_b")\
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
        this test in sqlite or oracle as they do not support CREATE DATABASE.
        """
        for backend in get_test_backends(exclude={'sqlite', 'oracle'}):
            migrations = read_migrations(tmpdir)
            backend.apply_migrations(migrations)
            backend.rollback_migrations(migrations)

    def test_lock(self, backend):
        """
        Test that :meth:`~yoyo.backends.DatabaseBackend.lock`
        acquires an exclusive lock
        """
        if backend.uri.scheme == 'sqlite':
            pytest.skip("Concurrency tests not supported for sqlite databases")

        lock_duration = 0.2

        def do_something_with_lock():
            with backend.lock():
                time.sleep(lock_duration)

        thread = Thread(target=do_something_with_lock)
        t = time.time()
        thread.start()
        # Give the thread time to acquire the lock, but not enough
        # to complete
        time.sleep(lock_duration * 0.2)
        with backend.lock():
            delta = time.time() - t
            assert delta >= lock_duration

        thread.join()

    def test_lock_times_out(self, backend):

        if backend.uri.scheme == 'sqlite':
            pytest.skip("Concurrency tests not supported for sqlite databases")

        def do_something_with_lock():
            with backend.lock():
                time.sleep(lock_duration)

        lock_duration = 2
        thread = Thread(target=do_something_with_lock)
        thread.start()
        # Give the thread time to acquire the lock, but not enough
        # to complete
        time.sleep(lock_duration * 0.1)
        with pytest.raises(exceptions.LockTimeout):
            with backend.lock(timeout=lock_duration * 0.1):
                assert False, "Execution should never reach this point"

        thread.join()


class TestInitConnection(object):

    class MockBackend(backends.DatabaseBackend):
        driver = Mock(DatabaseError=Exception, paramstyle='format')

        def list_tables(self):
            return []

        def connect(self, dburi):
            return Mock()

    def test_it_calls_init_connection(self):

        with patch('yoyo.internalmigrations.upgrade'), \
                patch.object(self.MockBackend, 'init_connection', Mock()) as mock_init:

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

        with patch('yoyo.internalmigrations.upgrade'):
            backend = MockPGBackend('', '')
            backend.rollback()
            assert backend.connection.cursor().execute.call_args == \
                    call('SET search_path TO foo')

    def test_postgresql_connects_with_schema(self):
        dburi = next(iter(get_test_dburis(only={'postgresql'})), None)
        if dburi is None:
            pytest.skip("PostgreSQL backend not available")
        backend = get_backend(dburi)
        with backend.transaction():
            backend.execute("CREATE SCHEMA foo")
        try:
            assert get_backend(dburi + '?schema=foo')\
                    .execute("SHOW search_path").fetchone() == ('foo',)
        finally:
            with backend.transaction():
                backend.execute("DROP SCHEMA foo CASCADE")
