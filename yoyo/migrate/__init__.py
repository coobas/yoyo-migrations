import os
import sys
import inspect
import logging

from itertools import count
from datetime import datetime
from logging import warn, info, debug

from yoyo.migrate.utils import plural

__version__ = '5dev'

class DatabaseError(Exception):
    pass

def with_placeholders(conn, paramstyle, sql):
    placeholder_gen = {
        'qmark': '?',
        'format': '%s',
        'pyformat': '%s',
    }.get(paramstyle)
    if placeholder_gen is None:
        raise ValueError("Unsupported parameter format %s" % paramstyle)
    return sql.replace('?', placeholder_gen)

class Migration(object):

    def __init__(self, id, steps, source):
        self.id = id
        self.steps = steps
        self.source = source

    def isapplied(self, conn, paramstyle):
        cursor = conn.cursor()
        try:
            cursor.execute(
                with_placeholders(conn, paramstyle, "SELECT COUNT(1) FROM _yoyo_migration WHERE id=?"),
                (self.id,)
            )
            return cursor.fetchone()[0] > 0
        finally:
            cursor.close()

    def apply(self, conn, paramstyle, force=False):
        info("Applying %s", self.id)
        Migration._process_steps(self.steps, conn, paramstyle, 'apply', force=force)
        cursor = conn.cursor()
        cursor.execute(
            with_placeholders(conn, paramstyle, "INSERT INTO _yoyo_migration (id, ctime) VALUES (?, ?)"),
            (self.id, datetime.now())
        )
        conn.commit()
        cursor.close()

    def rollback(self, conn, paramstyle, force=False):
        info("Rolling back %s", self.id)
        Migration._process_steps(reversed(self.steps), conn, paramstyle, 'rollback', force=force)
        cursor = conn.cursor()
        cursor.execute(
            with_placeholders(conn, paramstyle, "DELETE FROM _yoyo_migration WHERE id=?"),
            (self.id,)
        )
        conn.commit()
        cursor.close()

    @staticmethod
    def _process_steps(steps, conn, paramstyle, direction, force=False):

        reverse = {
            'rollback': 'apply',
            'apply': 'rollback',
        }[direction]

        executed_steps = []
        for step in steps:
            try:
                getattr(step, direction)(conn, paramstyle, force)
                executed_steps.append(step)
            except DatabaseError:
                conn.rollback()
                exc_info = sys.exc_info()
                try:
                    for step in reversed(executed_steps):
                        getattr(step, reverse)(conn, paramstyle)
                except DatabaseError:
                    logging.exception('Database error when reversing %s of step', direction)
                raise exc_info[0], exc_info[1], exc_info[2]

class PostApplyHookMigration(Migration):
    """
    A special migration that is run after successfully applying a set of migrations.
    Unlike a normal migration this will be run every time migrations are applied
    script is called.
    """

    def apply(self, conn, paramstyle, force=False):
        info("Applying %s", self.id)
        self.__class__._process_steps(
            self.steps,
            conn,
            paramstyle,
            'apply',
            force=True
        )

    def rollback(self, conn, paramstyle, force=False):
        info("Rolling back %s", self.id)
        self.__class__._process_steps(
            reversed(self.steps),
            conn,
            paramstyle,
            'rollback',
            force=True
        )

class StepBase(object):


    def apply(self, conn, paramstyle, force=False):
        raise NotImplementedError()

    def rollback(self, conn, paramstyle, force=False):
        raise NotImplementedError()

class Transaction(StepBase):
    """
    A ``Transaction`` object causes all associated steps to be run within a
    single database transaction.
    """

    def __init__(self, steps, ignore_errors=None):
        assert ignore_errors in (None, 'all', 'apply', 'rollback')
        self.steps = steps
        self.ignore_errors = ignore_errors

    def apply(self, conn, paramstyle, force=False):

        for step in self.steps:
            try:
                step.apply(conn, paramstyle, force)
            except DatabaseError:
                conn.rollback()
                if force or self.ignore_errors in ('apply', 'all'):
                    logging.exception("Ignored error in step %d", step.id)
                    return
                raise
        conn.commit()

    def rollback(self, conn, paramstyle, force=False):
        for step in reversed(self.steps):
            step.rollback(conn, paramstyle, force)
        conn.commit()

class MigrationStep(StepBase):
    """
    Model a single migration.

    Each migration step comprises apply and rollback steps of up and down SQL
    statements.
    """

    transaction = None

    def __init__(self, id, apply, rollback):

        self.id = id
        self._rollback = rollback
        self._apply = apply

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

    def apply(self, conn, paramstyle, force=False):
        """
        Apply the step.

        force
            If true, errors will be logged but not be re-raised
        """
        info(" - applying step %d", self.id)
        if not self._apply:
            return
        cursor = conn.cursor()
        try:
            if isinstance(self._apply, (str, unicode)):
                self._execute(cursor, self._apply)
            else:
                self._apply(conn)
        finally:
            cursor.close()

    def rollback(self, conn, paramstyle, force=False):
        """
        Rollback the step.
        """
        info(" - rolling back step %d", self.id)
        if self._rollback is None:
            return
        cursor = conn.cursor()
        try:
            try:
                for item in reversed(self._rollback):
                    if isinstance(item, (str, unicode)):
                        self._execute(cursor, self._rollback)
                    else:
                        item(conn)
                conn.commit()
            except DatabaseError:
                if force or self.ignore_errors in ('rollback', 'all'):
                    logging.exception("Ignoring error in step %d", self.id)
                    conn.rollback()
                else:
                    raise
        finally:
            cursor.close()


def read_migrations(conn, paramstyle, directory, names=None):
    """
    Return a ``MigrationList`` containing all migrations from ``directory``.
    If ``names`` is given, this only return migrations with names from the given list (without file extensions).
    """

    migrations = MigrationList(conn, paramstyle)
    paths = [
        os.path.join(directory, path) for path in os.listdir(directory) if path.endswith('.py')
    ]

    for path in sorted(paths):

        filename = os.path.splitext(os.path.basename(path))[0]

        if filename.startswith('post-apply'):
            migration_class = PostApplyHookMigration
        else:
            migration_class = Migration

        if migration_class is Migration and names is not None and filename not in names:
            continue

        step_id = count(0)
        transactions = []

        def step(apply, rollback=None, ignore_errors=None):
            """
            Wrap the given apply and rollback code in a transaction, and add it
            to the list of steps. Return the transaction-wrapped step.
            """
            t = Transaction([MigrationStep(step_id.next(), apply, rollback)], ignore_errors)
            transactions.append(t)
            return t

        def transaction(*steps, **kwargs):
            """
            Wrap the given list of steps in a single transaction, removing the
            default transactions around individual steps.
            """
            ignore_errors = kwargs.pop('ignore_errors', None)
            assert kwargs == {}

            transaction = Transaction([], ignore_errors)
            for oldtransaction in steps:
                if oldtransaction.ignore_errors is not None:
                    raise AssertionError("ignore_errors cannot be specified within a transaction")
                try:
                    (step,) = oldtransaction.steps
                except ValueError:
                    raise AssertionError("Transactions cannot be nested")
                transaction.steps.append(step)
                transactions.remove(oldtransaction)
            transactions.append(transaction)
            return transaction

        file = open(path, 'r')
        try:
            source = file.read()
            migration_code = compile(source, file.name, 'exec')
        finally:
            file.close()

        ns = {'step' : step, 'transaction': transaction}
        exec migration_code in ns
        migration = migration_class(os.path.basename(filename), transactions, source)
        if migration_class is PostApplyHookMigration:
            migrations.post_apply.append(migration)
        else:
            migrations.append(migration)

    return migrations


class MigrationList(list):
    """
    A list of database migrations.

    Use ``to_apply`` or ``to_rollback`` to retrieve subset lists of migrations
    that can be applied/rolled back.
    """


    def __init__(self, conn, paramstyle, items=None, post_apply=None):
        super(MigrationList, self).__init__(items if items else [])
        self.conn = conn
        self.paramstyle = paramstyle
        self.post_apply = post_apply if post_apply else []
        initialize_connection(self.conn)

    def to_apply(self):
        """
        Return a list of the subset of migrations not already applied.
        """
        return self.__class__(
            self.conn,
            self.paramstyle,
            [ m for m in self if not m.isapplied(self.conn, self.paramstyle) ],
            self.post_apply
        )

    def to_rollback(self):
        """
        Return a list of the subset of migrations already applied, which may be
        rolled back.

        The order of migrations will be reversed.
        """
        return self.__class__(
            self.conn,
            self.paramstyle,
            list(reversed([m for m in self if m.isapplied(self.conn, self.paramstyle)])),
            self.post_apply
        )

    def filter(self, predicate):
        return self.__class__(
            self.conn,
            self.paramstyle,
            [ m for m in self if predicate(m) ],
            self.post_apply
        )

    def replace(self, newmigrations):
        return self.__class__(self.conn, self.paramstyle, newmigrations, self.post_apply)

    def apply(self, force=False):
        if not self:
            return
        for m in self + self.post_apply:
            m.apply(self.conn, self.paramstyle, force)

    def rollback(self, force=False):
        if not self:
            return
        for m in self + self.post_apply:
            m.rollback(self.conn, self.paramstyle, force)

    def __getslice__(self, i, j):
        return self.__class__(
            self.conn,
            self.paramstyle,
            super(MigrationList, self).__getslice__(i, j),
            self.post_apply
        )

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


