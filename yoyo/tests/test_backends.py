import pytest
from threading import Thread
import time

from yoyo import backends, read_migrations
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
        for backend in get_test_backends(exclude={'sqlite'}):
            migrations = read_migrations(tmpdir)
            backend.apply_migrations(migrations)
            backend.rollback_migrations(migrations)

    @with_migrations(a="""
        steps = [
            step("SELECT pg_sleep(10)"),
        ]
    """)
    def test_lock_migration_table(self, tmpdir):
        backend = get_test_backends(only={'postgresql'})[0]
        migrations = read_migrations(tmpdir)
        Thread(target=backend.apply_migrations, args=(migrations,)).start()
        # give a chance to start, but wake up in the middle of applying
        time.sleep(0.1)
        assert backend.get_applied_migration_ids() == ['a']
