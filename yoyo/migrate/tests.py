import re
from tempfile import mkdtemp
from shutil import rmtree
from functools import wraps
import os

from yoyo.migrate.connections import connect, parse_uri, unparse_uri
from yoyo.migrate import read_migrations, create_migrations_table
from yoyo.migrate import DatabaseError

dburi = "sqlite:///:memory:"

def with_migrations(*migrations):
    """
    Decorator taking a list of migrations. Creates a temporary directory writes
    each migration to a file (named '0.py', '1.py', '2.py' etc), calls the decorated
    function with the directory name as the first argument, and cleans up the
    temporary directory on exit.
    """

    def unindent(s):
        initial_indent = re.search(r'^([ \t]*)\S', s, re.M).group(1)
        return re.sub(r'(^|[\r\n]){0}'.format(re.escape(initial_indent)), r'\1', s)

    def decorator(func):
        tmpdir = mkdtemp()
        for ix, code in enumerate(migrations):
            with open(os.path.join(tmpdir, '{0}.py'.format(ix)), 'w') as f:
                f.write(unindent(code).strip())

        @wraps(func)
        def decorated(*args, **kwargs):
            try:
                func(tmpdir, *args, **kwargs)
            finally:
                rmtree(tmpdir)

        return decorated
    return decorator

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
    except DatabaseError:
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
    step("UPDATE test SET id=2 WHERE id=1", "SELECT nonexistent FROM imaginary", ignore_errors='rollback')
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
    migrations = read_migrations(conn, paramstyle, tmpdir, migration_table='another_migration_table')
    migrations.apply()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM another_migration_table")
    assert cursor.fetchall() == [(u'0',)]

