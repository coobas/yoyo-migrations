import os
import sys
import inspect
import logging

from datetime import datetime
from logging import warn, info, debug

class DatabaseError(Exception):
    pass

def with_placeholders(conn, sql):
    placeholder_gen = {
        'qmark': '?',
        'format': '%s',
        'pyformat': '%s',
    }.get(inspect.getmodule(type(conn)).paramstyle)
    if placeholder_gen is None:
        raise ValueError("Unsupported parameter format %s" % conn.paramstyle)
    return sql.replace('?', placeholder_gen)

class Migration(object):

    def __init__(self, id, steps, source):
        self.id = id
        self.steps = steps
        self.source = source

    def isapplied(self, conn):
        cursor = conn.cursor()
        try:
            cursor.execute(
                with_placeholders(conn, "SELECT COUNT(1) FROM _yoyo_migration WHERE id=?"),
                (self.id,)
            )
            return cursor.fetchone()[0] > 0
        finally:
            cursor.close()

    def apply(self, conn, force=False):
        info("Applying %s", self.id)
        Migration._process_steps(self.steps, conn, 'apply', force=force)
        cursor = conn.cursor()
        cursor.execute(
            with_placeholders(conn, "INSERT INTO _yoyo_migration (id, ctime) VALUES (?, ?)"),
            (self.id, datetime.now())
        )
        conn.commit()
        cursor.close()

    def rollback(self, conn, force=False):
        info("Rolling back %s", self.id)
        Migration._process_steps(reversed(self.steps), conn, 'rollback', force=force)
        cursor = conn.cursor()
        cursor.execute(
            with_placeholders(conn, "DELETE FROM _yoyo_migration WHERE id=?"),
            (self.id,)
        )
        conn.commit()
        cursor.close()

    @staticmethod
    def _process_steps(steps, conn, direction, force=False):

        reverse = {
            'rollback': 'apply',
            'apply': 'rollback',
        }[direction]

        executed_steps = []
        for ix, step in enumerate(steps):
            try:
                if getattr(step, direction)(conn, force):
                    executed_steps.append(step)
            except DatabaseError:
                conn.rollback()
                exc_info = sys.exc_info()
                try:
                    for step in reversed(executed_steps):
                        getattr(step, reverse)(conn)
                except DatabaseError:
                    logging.exception('Database error when reversing %s  of step', direction)
                raise exc_info[0], exc_info[1], exc_info[2]


class MigrationStep(object):
    """
    Model a single migration. Each migration step comprises a single apply and
    rollback step of up and down SQL statements.
    """
    def __init__(self, id, apply, rollback, ignore_errors):

        assert ignore_errors in (None, 'all', 'apply', 'rollback')

        self.id = id
        self._rollback = rollback
        self._apply = apply
        self.ignore_errors = ignore_errors

    def _execute(self, cursor, stmt, out=sys.stdout):
        """
        Execute the given statement. If rows are returned, output these in a
        tabulated format.
        """
        if isinstance(stmt, unicode):
            debug(" - executing %r", stmt.encode('ascii', 'replace'))
        else:
            debug(" - executing %r", stmt)
        cursor.execute(stmt)
        if cursor.description:
            result = [
                [unicode(value) for value in row] for row in cursor.fetchall()
            ]
            column_names = [ desc[0] for desc in cursor.description ]
            column_sizes = [ len(c) for c in column_names ]

            for row in result:
                for ix, value in enumerate(row):
                    if len(value) > column_sizes[ix]:
                        column_sizes[ix] = len(value)
            format = '|'.join(' %%- %ds ' % size for size in column_sizes)
            out.write(format % tuple(column_names) + "\n")
            out.write('+'.join('-' * (size + 2) for size in column_sizes) + "\n")
            for row in result:
                out.write((format % tuple(row)).encode('utf8') + "\n")
            out.write(plural(len(result), '(%d row)', '(%d rows)') + "\n")

    def apply(self, conn, force=False):
        """
        Apply the step and commit the change. Return ``True`` if a change was
        successfully applied, ``False`` if there was no apply action for this
        step.
        """
        info(" - applying step %d", self.id)
        if self._apply is None:
            return False
        cursor = conn.cursor()
        try:
            try:
                if callable(self._apply):
                    self._apply(conn)
                else:
                    self._execute(cursor, self._apply)
                conn.commit()
            except DatabaseError:
                if force or self.ignore_errors in ('apply', 'all'):
                    conn.rollback()
                    logging.exception("Ignored error in step %d", self.id)
                else:
                    raise
        finally:
            cursor.close()
        return True

    def rollback(self, conn, force=False):
        """
        Rollback the step and commit the change. Return ``True`` if a change
        was successfully rolled back, ``False`` if there was no rollback action
        for this step.
        """
        info(" - rolling back step %d", self.id)
        if self._rollback is None:
            return
        cursor = conn.cursor()
        try:
            try:
                if callable(self._rollback):
                    self._rollback(cursor)
                else:
                    self._execute(cursor, self._rollback)
                conn.commit()
            except DatabaseError:
                if force or self.ignore_errors in ('rollback', 'all'):
                    logging.exception("Ignoring error in step %d", self.id)
                    conn.rollback()
                else:
                    raise
        finally:
            cursor.close()


def read_migrations(conn, directory, names=None):
    """
    Return a MigrationList of migrations from ``directory``, optionally
    limiting to those with the specified filenames (without extensions).
    """

    result = MigrationList(conn, [])
    paths = [
        os.path.join(directory, path) for path in os.listdir(directory) if path.endswith('.py')
    ]

    for path in sorted(paths):

        filename = os.path.splitext(os.path.basename(path))[0]
        if names is not None and filename not in names:
            continue

        steps = []
        def step(apply, rollback=None, ignore_errors=None):
            steps.append(MigrationStep(len(steps), apply, rollback, ignore_errors))

        file = open(path, 'r')
        try:
            source = file.read()
            migration = compile(source, file.name, 'exec')
        finally:
            file.close()
        ns = { 'step' : step }
        exec migration in ns
        result.append(Migration(os.path.basename(filename), steps, source))
    return result


class MigrationList(list):

    def __init__(self, conn, items):
        super(MigrationList, self).__init__(items)
        self.conn = conn
        initialize_connection(self.conn)

    def to_apply(self):
        """
        Subset of migrations that have not already been applied
        """
        return self.__class__(
            self.conn,
            [ m for m in self if not m.isapplied(self.conn) ]
        )

    def to_rollback(self):
        """
        Subset of migrations that have been applied and may be rolled back
        """
        return self.__class__(
            self.conn,
            [ m for m in self if m.isapplied(self.conn) ]
        )

    def filter(self, predicate):
        return self.__class__(
            self.conn,
            [ m for m in self if predicate(m) ]
        )

    def replace(self, newmigrations):
        return self.__class__(self.conn, newmigrations)

    def apply(self, force=False):
        for m in self:
            m.apply(self.conn, force)

    def rollback(self, force=False):
        for m in self:
            m.rollback(self.conn, force)


def create_migrations_table(conn):
    """
    Create a database table to track migrations
    """
    try:
        cursor = conn.cursor()
        try:
            try:
                cursor.execute("""
                    CREATE TABLE _yoyo_migration (id VARCHAR(255) NOT NULL PRIMARY KEY, ctime TIMESTAMP)
                """)
                conn.commit()
            except DatabaseError:
                pass
        finally:
            cursor.close()
    finally:
        conn.rollback()


def initialize_connection(conn):
    """
    Initialize the DBAPI connection for use.

    - Installs ``yoyo.migrate.DatabaseError`` as a base class for the
      connection's own DatabaseError

    - Creates the migrations table if not already existing

    """
    module = inspect.getmodule(type(conn))
    if DatabaseError not in module.DatabaseError.__bases__:
        module.DatabaseError.__bases__ += (DatabaseError,)
    create_migrations_table(conn)


