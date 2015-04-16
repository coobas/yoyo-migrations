import pytest
from mock import Mock

from yoyo.connections import connect
from yoyo import read_migrations
from yoyo import exceptions

from yoyo.tests import with_migrations, dburi
from yoyo.migrations import topological_sort


@with_migrations(
    """
step("CREATE TABLE test (id INT)")
transaction(
    step("INSERT INTO test VALUES (1)"),
    step("INSERT INTO test VALUES ('x', 'y')")
)
    """
)
def test_transaction_is_not_committed_on_error(tmpdir):
    conn, paramstyle = connect(dburi)
    migrations = read_migrations(conn, paramstyle, tmpdir)
    try:
        migrations.apply()
    except tuple(exceptions.DatabaseErrors):
        # Expected
        pass
    else:
        raise AssertionError("Expected a DatabaseError")
    cursor = conn.cursor()
    cursor.execute("SELECT count(1) FROM test")
    assert cursor.fetchone() == (0,)


@with_migrations(
    'step("CREATE TABLE test (id INT)")',
    '''
step("INSERT INTO test VALUES (1)", "DELETE FROM test WHERE id=1")
step("UPDATE test SET id=2 WHERE id=1", "UPDATE test SET id=1 WHERE id=2")
    '''
)
def test_rollbacks_happen_in_reverse(tmpdir):
    conn, paramstyle = connect(dburi)
    migrations = read_migrations(conn, paramstyle, tmpdir)
    migrations.apply()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM test")
    assert cursor.fetchall() == [(2,)]
    migrations.rollback()
    cursor.execute("SELECT * FROM test")
    assert cursor.fetchall() == []


@with_migrations(
    '''
    step("CREATE TABLE test (id INT)")
    step("INSERT INTO test VALUES (1)")
    step("INSERT INTO test VALUES ('a', 'b')", ignore_errors='all')
    step("INSERT INTO test VALUES (2)")
    '''
)
def test_execution_continues_with_ignore_errors(tmpdir):
    conn, paramstyle = connect(dburi)
    migrations = read_migrations(conn, paramstyle, tmpdir)
    migrations.apply()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM test")
    assert cursor.fetchall() == [(1,), (2,)]


@with_migrations(
    '''
    step("CREATE TABLE test (id INT)")
    transaction(
        step("INSERT INTO test VALUES (1)"),
        step("INSERT INTO test VALUES ('a', 'b')"),
        ignore_errors='all'
    )
    step("INSERT INTO test VALUES (2)")
    '''
)
def test_execution_continues_with_ignore_errors_in_transaction(tmpdir):
    conn, paramstyle = connect(dburi)
    migrations = read_migrations(conn, paramstyle, tmpdir)
    migrations.apply()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM test")
    assert cursor.fetchall() == [(2,)]


@with_migrations(
    '''
    step("CREATE TABLE test (id INT)")
    step("INSERT INTO test VALUES (1)", "DELETE FROM test WHERE id=2")
    step("UPDATE test SET id=2 WHERE id=1",
         "SELECT nonexistent FROM imaginary", ignore_errors='rollback')
    '''
)
def test_rollbackignores_errors(tmpdir):
    conn, paramstyle = connect(dburi)
    migrations = read_migrations(conn, paramstyle, tmpdir)
    migrations.apply()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM test")
    assert cursor.fetchall() == [(2,)]

    migrations.rollback()
    cursor.execute("SELECT * FROM test")
    assert cursor.fetchall() == []


@with_migrations(
    '''
    step("CREATE TABLE test (id INT)")
    step("DROP TABLE test")
    '''
)
def test_specify_migration_table(tmpdir):
    conn, paramstyle = connect(dburi)
    migrations = read_migrations(conn, paramstyle, tmpdir,
                                 migration_table='another_migration_table')
    migrations.apply()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM another_migration_table")
    assert cursor.fetchall() == [('0',)]


@with_migrations(
    '''
    def foo(conn):
        conn.cursor().execute("CREATE TABLE foo_test (id INT)")
        conn.cursor().execute("INSERT INTO foo_test VALUES (1)")
        conn.commit()
    def bar(conn):
        foo(conn)
    step(bar)
    '''
)
def test_migration_functions_have_namespace_access(tmpdir):
    """
    Test that functions called via step have access to the script namespace
    """
    conn, paramstyle = connect(dburi)
    migrations = read_migrations(conn, paramstyle, tmpdir,
                                 migration_table='another_migration_table')
    migrations.apply()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM foo_test")
    assert cursor.fetchall() == [(1,)]


@with_migrations(
    '''
    from yoyo import transaction, step
    step("CREATE TABLE test (id INT)")
    transaction(step("INSERT INTO test VALUES (1)")),
    '''
)
def test_migrations_can_import_step_and_transaction(tmpdir):
    conn, paramstyle = connect(dburi)
    migrations = read_migrations(conn, paramstyle, tmpdir,
                                 migration_table='another_migration_table')
    migrations.apply()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM test")
    assert cursor.fetchall() == [(1,)]


class TestTopologicalSort(object):

    def get_mock_migrations(self):
        return [Mock(id='m1', depends=set()), Mock(id='m2', depends=set()),
                Mock(id='m3', depends=set()), Mock(id='m4', depends=set())]

    def test_it_keeps_stable_order(self):
        m1, m2, m3, m4 = self.get_mock_migrations()
        assert list(topological_sort([m1, m2, m3, m4])) == [m1, m2, m3, m4]
        assert list(topological_sort([m4, m3, m2, m1])) == [m4, m3, m2, m1]

    def test_it_sorts_topologically(self):
        m1, m2, m3, m4 = self.get_mock_migrations()
        m3.depends.add(m4)
        assert list(topological_sort([m1, m2, m3, m4])) == [m4, m3, m1, m2]

    def test_it_brings_depended_upon_migrations_to_the_front(self):
        m1, m2, m3, m4 = self.get_mock_migrations()
        m1.depends.add(m4)
        print(list(m.id for m in topological_sort([m1, m2, m3, m4])))
        assert list(topological_sort([m1, m2, m3, m4])) == [m4, m1, m2, m3]

    def test_it_discards_missing_dependencies(self):
        m1, m2, m3, m4 = self.get_mock_migrations()
        m3.depends.add(Mock())
        assert list(topological_sort([m1, m2, m3, m4])) == [m1, m2, m3, m4]

    def test_it_catches_cycles(self):
        m1, m2, m3, m4 = self.get_mock_migrations()
        m3.depends.add(m3)
        with pytest.raises(exceptions.BadMigration):
            list(topological_sort([m1, m2, m3, m4]))
