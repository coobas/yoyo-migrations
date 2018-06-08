import pytest
from threading import Thread
import time

from yoyo import backends
from yoyo import read_migrations
from yoyo import exceptions
from yoyo.tests import get_test_backends
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
