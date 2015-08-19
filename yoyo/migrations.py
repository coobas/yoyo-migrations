# Copyright 2015 Oliver Cope
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections import defaultdict, OrderedDict
from datetime import datetime
from itertools import chain, count
from logging import getLogger
import os
import sys
import inspect

from yoyo.compat import reraise, exec_, ustr, stdout
from yoyo import exceptions
from yoyo.utils import plural

logger = getLogger(__name__)
default_migration_table = '_yoyo_migration'
_step_collectors = {}


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

    __all_migrations = {}

    def __init__(self, id, path):
        self.id = id
        self.path = path
        self.steps = None
        self.source = None
        self._depends = None
        self.__all_migrations[id] = self

    @property
    def loaded(self):
        return self.steps is not None

    @property
    def depends(self):
        self.load()
        return self._depends

    def load(self):
        if self.loaded:
            return
        with open(self.path, 'r') as f:
            self.source = source = f.read()
            migration_code = compile(source, f.name, 'exec')

        collector = _step_collectors[f.name] = StepCollector()
        ns = {'step': collector.step,
              'transaction': collector.transaction}
        try:
            exec_(migration_code, ns)
        except Exception as e:
            logger.exception("Could not import migration from %r: %r",
                             self.path, e)
            raise exceptions.BadMigration(self.path, e)
        depends = ns.get('__depends__', [])
        if isinstance(depends, (ustr, bytes)):
            depends = [depends]
        self._depends = {self.__all_migrations.get(id, None)
                         for id in depends}
        if None in self._depends:
            raise exceptions.BadMigration(
                "Could not resolve dependencies in {}".format(self.path))
        self.source = source
        self.steps = collector.steps

    def isapplied(self, conn, paramstyle, migration_table):
        cursor = conn.cursor()
        try:
            cursor.execute(
                with_placeholders(conn, paramstyle, "SELECT COUNT(1) FROM " +
                                  migration_table + " WHERE id=?"),
                (self.id,)
            )
            return cursor.fetchone()[0] > 0
        finally:
            cursor.close()

    def apply(self, conn, paramstyle, migration_table, force=False):
        logger.info("Applying %s", self.id)
        self.load()
        Migration._process_steps(self.steps, conn, paramstyle, 'apply',
                                 force=force)
        cursor = conn.cursor()
        cursor.execute(
            with_placeholders(conn, paramstyle, "INSERT INTO " +
                              migration_table + " (id, ctime) VALUES (?, ?)"),
            (self.id, datetime.utcnow())
        )
        conn.commit()
        cursor.close()

    def rollback(self, conn, paramstyle, migration_table, force=False):
        logger.info("Rolling back %s", self.id)
        self.load()
        Migration._process_steps(reversed(self.steps), conn, paramstyle,
                                 'rollback', force=force)
        cursor = conn.cursor()
        cursor.execute(
            with_placeholders(conn, paramstyle, "DELETE FROM " +
                              migration_table + " WHERE id=?"),
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
            except tuple(exceptions.DatabaseErrors):
                conn.rollback()
                exc_info = sys.exc_info()
                try:
                    for step in reversed(executed_steps):
                        getattr(step, reverse)(conn, paramstyle)
                except tuple(exceptions.DatabaseErrors):
                    logger.exception(
                        'Database error when reversing %s of step', direction)
                reraise(exc_info[0], exc_info[1], exc_info[2])


class PostApplyHookMigration(Migration):
    """
    A special migration that is run after successfully applying a set of
    migrations. Unlike a normal migration this will be run every time
    migrations are applied script is called.
    """

    def apply(self, conn, paramstyle, migration_table, force=False):
        logger.info("Applying %s", self.id)
        self.__class__._process_steps(
            self.steps,
            conn,
            paramstyle,
            'apply',
            force=True
        )

    def rollback(self, conn, paramstyle, migration_table, force=False):
        logger.info("Rolling back %s", self.id)
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
            except tuple(exceptions.DatabaseErrors):
                conn.rollback()
                if force or self.ignore_errors in ('apply', 'all'):
                    logger.exception("Ignored error in step %d", step.id)
                    return
                raise
        conn.commit()

    def rollback(self, conn, paramstyle, force=False):
        for step in reversed(self.steps):
            try:
                step.rollback(conn, paramstyle, force)
            except tuple(exceptions.DatabaseErrors):
                conn.rollback()
                if force or self.ignore_errors in ('rollback', 'all'):
                    logger.exception("Ignored error in step %d", step.id)
                    return
                raise
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

    def _execute(self, cursor, stmt, out=stdout):
        """
        Execute the given statement. If rows are returned, output these in a
        tabulated format.
        """
        if isinstance(stmt, ustr):
            logger.debug(" - executing %r", stmt.encode('ascii', 'replace'))
        else:
            logger.debug(" - executing %r", stmt)
        cursor.execute(stmt)
        if cursor.description:
            result = [[ustr(value) for value in row]
                      for row in cursor.fetchall()]
            column_names = [desc[0] for desc in cursor.description]
            column_sizes = [len(c) for c in column_names]

            for row in result:
                for ix, value in enumerate(row):
                    if len(value) > column_sizes[ix]:
                        column_sizes[ix] = len(value)
            format = '|'.join(' %%- %ds ' % size for size in column_sizes)
            out.write(format % tuple(column_names) + "\n")
            out.write('+'.join('-' * (size + 2) for size in column_sizes)
                      + "\n")
            for row in result:
                out.write(format % tuple(row))
            out.write(plural(len(result), '(%d row)', '(%d rows)') + "\n")

    def apply(self, conn, paramstyle, force=False):
        """
        Apply the step.

        :param force: If true, errors will be logged but not be re-raised
        """
        logger.info(" - applying step %d", self.id)
        if not self._apply:
            return
        cursor = conn.cursor()
        try:
            if isinstance(self._apply, (ustr, str)):
                self._execute(cursor, self._apply)
            else:
                self._apply(conn)
        finally:
            cursor.close()

    def rollback(self, conn, paramstyle, force=False):
        """
        Rollback the step.
        """
        logger.info(" - rolling back step %d", self.id)
        if self._rollback is None:
            return
        cursor = conn.cursor()
        try:
            if isinstance(self._rollback, (ustr, str)):
                self._execute(cursor, self._rollback)
            else:
                self._rollback(conn)
        finally:
            cursor.close()


def read_migrations(conn, paramstyle, directory, names=None,
                    migration_table=default_migration_table):
    """
    Return a ``MigrationList`` containing all migrations from ``directory``.
    If ``names`` is given, this only return migrations with names from the
    given list (without file extensions).
    """
    migrations = []
    paths = [os.path.join(directory, path)
             for path in os.listdir(directory) if path.endswith('.py')]

    for path in sorted(paths):

        filename = os.path.splitext(os.path.basename(path))[0]

        if filename.startswith('post-apply'):
            migration_class = PostApplyHookMigration
        else:
            migration_class = Migration

        if migration_class is Migration and \
                names is not None and filename not in names:
            continue

        migration = migration_class(
            os.path.splitext(os.path.basename(path))[0], path)
        if migration_class is PostApplyHookMigration:
            migrations.post_apply.append(migration)
        else:
            migrations.append(migration)

    return MigrationList(conn, paramstyle, migration_table, items=migrations)


class MigrationList(list):
    """
    A list of database migrations.

    Use ``to_apply`` or ``to_rollback`` to retrieve subset lists of migrations
    that can be applied/rolled back.
    """

    def __init__(self, conn, paramstyle, migration_table, items=None,
                 post_apply=None):
        super(MigrationList, self).__init__(items if items else [])
        self.conn = conn
        self.paramstyle = paramstyle
        self.migration_table = migration_table
        self.post_apply = post_apply if post_apply else []
        initialize_connection(self.conn, migration_table)

    def to_apply(self):
        """
        Return a list of the subset of migrations not already applied.
        """
        return self.__class__(
            self.conn,
            self.paramstyle,
            self.migration_table,
            topological_sort(m for m in self
                             if not m.isapplied(self.conn, self.paramstyle,
                                                self.migration_table)),
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
            self.migration_table,
            reversed(topological_sort(
                m for m in self
                if m.isapplied(self.conn,
                               self.paramstyle,
                               self.migration_table))),
            self.post_apply
        )

    def filter(self, predicate):
        return self.__class__(
            self.conn,
            self.paramstyle,
            self.migration_table,
            [m for m in self if predicate(m)],
            self.post_apply
        )

    def replace(self, newmigrations):
        return self.__class__(self.conn, self.paramstyle, self.migration_table,
                              newmigrations, self.post_apply)

    def apply(self, force=False):
        if not self:
            return
        for m in self + self.post_apply:
            try:
                m.apply(
                    self.conn, self.paramstyle, self.migration_table, force)
            except exceptions.BadMigration:
                continue

    def rollback(self, force=False):
        if not self:
            return
        for m in self + self.post_apply:
            try:
                m.rollback(
                    self.conn, self.paramstyle, self.migration_table, force)
            except exceptions.BadMigration:
                continue

    def __getslice__(self, i, j):
        return self.__class__(
            self.conn,
            self.paramstyle,
            self.migration_table,
            super(MigrationList, self).__getslice__(i, j),
            self.post_apply
        )


def create_migrations_table(conn, tablename):
    """
    Create a database table to track migrations
    """
    try:
        cursor = conn.cursor()
        try:
            try:
                cursor.execute("""
                    CREATE TABLE %s (id VARCHAR(255) NOT NULL PRIMARY KEY,
                                     ctime TIMESTAMP)
                """ % (tablename,))
                conn.commit()
            except tuple(exceptions.DatabaseErrors):
                pass
        finally:
            cursor.close()
    finally:
        conn.rollback()


def initialize_connection(conn, tablename):
    """
    Initialize the connection for use by creating the migrations table if
    it does not already exist.
    """
    create_migrations_table(conn, tablename)


class StepCollector(object):
    """
    Provide the ``step`` and ``transaction`` functions used in migration
    scripts.

    Each call to step/transaction updates the StepCollector's ``steps`` list.
    """

    def __init__(self):
        self.steps = []
        self.step_id = count(0)

    def step(self, apply, rollback=None, ignore_errors=None):
        """
        Wrap the given apply and rollback code in a transaction, and add it
        to the list of steps.
        Return the transaction-wrapped step.
        """
        t = Transaction([MigrationStep(next(self.step_id), apply, rollback)],
                        ignore_errors)
        self.steps.append(t)
        return t

    def transaction(self, *steps, **kwargs):
        """
        Wrap the given list of steps in a single transaction, removing the
        default transactions around individual steps.
        """
        ignore_errors = kwargs.pop('ignore_errors', None)
        assert kwargs == {}

        transaction = Transaction([], ignore_errors)
        for oldtransaction in steps:
            if oldtransaction.ignore_errors is not None:
                raise AssertionError("ignore_errors cannot be specified "
                                        "within a transaction")
            try:
                (step,) = oldtransaction.steps
            except ValueError:
                raise AssertionError("Transactions cannot be nested")
            transaction.steps.append(step)
            self.steps.remove(oldtransaction)
        self.steps.append(transaction)
        return transaction


def step(*args, **kwargs):
    fi = inspect.getframeinfo(inspect.stack()[1][0])
    return _step_collectors[fi.filename].step(*args, **kwargs)


def transaction(*args, **kwargs):
    fi = inspect.getframeinfo(inspect.stack()[1][0])
    return _step_collectors[fi.filename].transaction(*args, **kwargs)


def topological_sort(migration_list):

    # The sorted list, initially empty
    L = list()

    # Make a copy of migration_list. It's probably an iterator.
    migration_list = list(migration_list)
    valid_migrations = set(migration_list)

    # Track graph edges in two parallel data structures.
    # Use OrderedDict so that we can traverse edges in order
    # and keep the sort stable
    forward_edges = defaultdict(OrderedDict)
    backward_edges = defaultdict(OrderedDict)

    for m in migration_list:
        for n in m.depends:
            if n not in valid_migrations:
                continue
            forward_edges[n][m] = 1
            backward_edges[m][n] = 1

    # Only toposort the migrations forming part of the dependency graph
    to_toposort = set(chain(forward_edges, backward_edges))

    # Starting migrations: those with no dependencies
    # This is a reversed list so that popping/pushing from the end maintains
    # the desired order
    S = list(reversed([m for m in to_toposort
                       if not any(n in valid_migrations for n in m.depends)]))

    while S:
        n = S.pop()
        L.append(n)

        # for each node M with an edge E from N to M
        for m in forward_edges[n]:

            # remove edge E from the graph
            del forward_edges[n][m]
            del backward_edges[m][n]

            # If M has no other incoming edges, it qualifies as a starting node
            if not backward_edges[m]:
                S.append(m)

    if any(forward_edges.values()):
        raise exceptions.BadMigration(
            "Circular dependencies among these migrations {}".format(
                ', '.join(m.id
                          for m in forward_edges
                          for n in {m} | set(forward_edges[m]))))

    # Return the toposorted migrations followed by the remainder of migrations
    # in their original order
    return L + [m for m in migration_list if m not in to_toposort]
