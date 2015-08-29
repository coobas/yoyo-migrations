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

from collections import defaultdict, OrderedDict, Counter, MutableSequence
from copy import copy
from itertools import chain, count
from logging import getLogger
import os
import sys
import inspect

from yoyo.compat import reraise, exec_, ustr, stdout
from yoyo import exceptions
from yoyo.utils import plural

logger = getLogger('yoyo.migrations')
default_migration_table = '_yoyo_migration'
_step_collectors = {}


class Migration(object):

    __all_migrations = {}

    def __init__(self, id, path):
        self.id = id
        self.path = path
        self.steps = None
        self.source = None
        self._depends = None
        self.__all_migrations[id] = self

    def __repr__(self):
        return '<{} {!r} from {}>'.format(self.__class__.__name__,
                                        self.id,
                                        self.path)

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

    def process_steps(self, backend, direction, force=False):

        self.load()
        reverse = {'rollback': 'apply',
                   'apply': 'rollback'}[direction]

        steps = self.steps
        if direction == 'rollback':
            steps = reversed(steps)

        executed_steps = []
        for step in steps:
            try:
                getattr(step, direction)(backend, force)
                executed_steps.append(step)
            except tuple(exceptions.DatabaseErrors):
                backend.connection.rollback()
                exc_info = sys.exc_info()
                try:
                    for step in reversed(executed_steps):
                        getattr(step, reverse)(backend)
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


class StepBase(object):

    def apply(self, backend, force=False):
        raise NotImplementedError()

    def rollback(self, backend, force=False):
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

    def apply(self, backend, force=False):

        for step in self.steps:
            try:
                step.apply(backend, force)
            except tuple(exceptions.DatabaseErrors):
                backend.rollback()
                if force or self.ignore_errors in ('apply', 'all'):
                    logger.exception("Ignored error in step %d", step.id)
                    return
                raise
        backend.commit()

    def rollback(self, backend, force=False):
        for step in reversed(self.steps):
            try:
                step.rollback(backend, force)
            except tuple(exceptions.DatabaseErrors):
                backend.rollback()
                if force or self.ignore_errors in ('rollback', 'all'):
                    logger.exception("Ignored error in step %d", step.id)
                    return
                raise
        backend.commit()


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

    def apply(self, backend, force=False):
        """
        Apply the step.

        :param force: If true, errors will be logged but not be re-raised
        """
        logger.info(" - applying step %d", self.id)
        if not self._apply:
            return
        if isinstance(self._apply, (ustr, str)):
            cursor = backend.cursor()
            try:
                self._execute(cursor, self._apply)
            finally:
                cursor.close()
        else:
            self._apply(backend.connection)

    def rollback(self, backend, force=False):
        """
        Rollback the step.
        """
        logger.info(" - rolling back step %d", self.id)
        if self._rollback is None:
            return
        if isinstance(self._rollback, (ustr, str)):
            cursor = backend.cursor()
            try:
                    self._execute(cursor, self._rollback)
            finally:
                cursor.close()
        else:
            self._rollback(backend.connection)


def read_migrations(*directories):
    """
    Return a ``MigrationList`` containing all migrations from ``directory``.
    """
    migrations = []
    for directory in directories:
        paths = [os.path.join(directory, path)
                for path in os.listdir(directory) if path.endswith('.py')]

        for path in sorted(paths):

            filename = os.path.splitext(os.path.basename(path))[0]

            if filename.startswith('post-apply'):
                migration_class = PostApplyHookMigration
            else:
                migration_class = Migration

            migration = migration_class(
                os.path.splitext(os.path.basename(path))[0], path)
            if migration_class is PostApplyHookMigration:
                migrations.post_apply.append(migration)
            else:
                migrations.append(migration)

    return MigrationList(migrations)


class MigrationList(MutableSequence):
    """
    A list of database migrations.
    """

    def __init__(self, items=None, post_apply=None):
        self.items = list(items) if items else []
        self.post_apply = post_apply if post_apply else []
        self.keys = set(item.id for item in items)
        self.check_conflicts()

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, repr(self.items))

    def check_conflicts(self):
        c = Counter()
        for item in self:
            c[item.id] += 1
            if c[item.id] > 1:
                raise exceptions.MigrationConflict(item.id)

    def __getitem__(self, n):
        if isinstance(n, slice):
            return self.__class__(self.items.__getitem__(n))
        return self.items.__getitem__(n)

    def __setitem__(self, n, ob):
        removing = self.items[n]
        if not isinstance(removing, list):
            remove_ids = set([item.id for item in removing])
            new_ids = [ob.id]
        else:
            remove_ids = set(item.id for item in removing)
            new_ids = {item.id for item in ob}

        for id in new_ids:
            if id in self.keys and id not in remove_ids:
                raise exceptions.MigrationConflict(id)

        self.keys.difference_update(removing)
        self.keys.update(new_ids)
        return self.items.__setitem__(n, ob)

    def __len__(self):
        return len(self.items)

    def __delitem__(self, i):
        self.keys.remove(self.items[i].id)
        self.items.__delitem__(i)

    def insert(self, i, x):
        if x.id in self.keys:
            raise exceptions.MigrationConflict(x.id)
        self.keys.add(x.id)
        return self.items.insert(i, x)

    def __add__(self, other):
        ob = copy(self)
        ob.extend(other)
        return ob

    def filter(self, predicate):
        return self.__class__([m for m in self if predicate(m)],
                              self.post_apply)

    def replace(self, newmigrations):
        return self.__class__(newmigrations, self.post_apply)

    def __getslice__(self, i, j):
        return self.__class__(super(MigrationList, self).__getslice__(i, j),
                              self.post_apply)


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
