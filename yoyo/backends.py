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

from datetime import datetime
from contextlib import contextmanager
from importlib import import_module
from itertools import count
from logging import getLogger

from . import exceptions, utils
from .migrations import topological_sort

logger = getLogger('yoyo.migrations')


class TransactionManager(object):
    """
    Returned by the :meth:`~yoyo.backends.DatabaseBackend.transaction`
    context manager.

    If rollback is called, the transaction is flagged to be rolled back
    when the context manager block closes
    """

    def __init__(self, backend):
        self.backend = backend
        self._rollback = False

    def __enter__(self):
        self._do_begin()
        return self

    def __exit__(self, exc_type, value, traceback):
        if exc_type:
            self._do_rollback()
            return None

        if self._rollback:
            self._do_rollback()
        else:
            self._do_commit()

    def rollback(self):
        """
        Flag that the transaction will be rolled back when the with statement
        exits
        """
        self._rollback = True

    def _do_begin(self):
        """
        Instruct the backend to begin a transaction
        """
        self.backend.begin()

    def _do_commit(self):
        """
        Instruct the backend to commit the transaction
        """
        self.backend.commit()

    def _do_rollback(self):
        """
        Instruct the backend to roll back the transaction
        """
        self.backend.rollback()


class SavepointTransactionManager(TransactionManager):

    id = None
    id_generator = count(1)

    def _do_begin(self):
        assert self.id is None
        self.id = 'sp_{}'.format(next(self.id_generator))
        self.backend.savepoint(self.id)

    def _do_commit(self):
        """
        This does nothing.

        Trying to the release savepoint here could cause an database error in
        databases where DDL queries cause the transaction to be committed
        and all savepoints released.
        """

    def _do_rollback(self):
        self.backend.savepoint_rollback(self.id)


class DatabaseBackend(object):

    driver_module = None
    connection = None
    create_table_sql = """
        CREATE TABLE {table_name} (
            id VARCHAR(255) NOT NULL PRIMARY KEY,
            ctime TIMESTAMP
        )"""
    list_tables_sql = "SELECT table_name FROM information_schema.tables"
    is_applied_sql = "SELECT COUNT(1) FROM {0.migration_table} WHERE id=?"
    insert_migration_sql = ("INSERT INTO {0.migration_table} (id, ctime) "
                            "VALUES (?, ?)")
    delete_migration_sql = "DELETE FROM {0.migration_table} WHERE id=?"
    applied_ids_sql = "SELECT id FROM {0.migration_table} ORDER by ctime"
    create_test_table_sql = "CREATE TABLE {table_name} (id INT PRIMARY KEY)"

    _driver = None
    _in_transaction = False

    def __init__(self, dburi, migration_table):
        self.uri = dburi
        self.DatabaseError = self.driver.DatabaseError
        self._connection = self.connect(dburi)
        self.migration_table = migration_table
        self.create_migrations_table()
        self.has_transactional_ddl = self._check_transactional_ddl()

    def _load_driver_module(self):
        """
        Load the dbapi driver module and register the base exception class
        """
        driver = import_module(self.driver_module)
        exceptions.register(driver.DatabaseError)
        return driver

    @property
    def driver(self):
        if self._driver:
            return self._driver
        self._driver = self._load_driver_module()
        return self._driver

    @property
    def connection(self):
        return self._connection

    def _check_transactional_ddl(self):
        """
        Return True if the database supports committing/rolling back
        DDL statements within a transaction
        """
        table_name = '_yoyo_tmp_{}'.format(utils.get_random_string(10))
        sql = self.create_test_table_sql.format(table_name=table_name)
        with self.transaction() as t:
            self.execute(sql)
            t.rollback()
        try:
            with self.transaction():
                self.execute("DROP TABLE {}".format(table_name))
        except self.DatabaseError:
            return True
        return False

    def list_tables(self):
        """
        Return a list of tables present in the backend.
        This is used by the test suite to clean up tables
        generated during testing
        """
        cursor = self.execute(self.list_tables_sql)
        return [row[0] for row in cursor.fetchall()]

    def transaction(self):
        if not self._in_transaction:
            return TransactionManager(self)

        else:
            return SavepointTransactionManager(self)

    def cursor(self):
        return self.connection.cursor()

    def commit(self):
        self.connection.commit()
        self._in_transaction = False

    def rollback(self):
        self.connection.rollback()
        self._in_transaction = False

    def begin(self):
        """
        Begin a new transaction
        """
        self._in_transaction = True
        self.execute("BEGIN")

    def savepoint(self, id):
        """
        Create a new savepoint with the given id
        """
        self.execute("SAVEPOINT {}".format(id))

    def savepoint_release(self, id):
        """
        Release (commit) the savepoint with the given id
        """
        self.execute("RELEASE SAVEPOINT {}".format(id))

    def savepoint_rollback(self, id):
        """
        Rollback the savepoint with the given id
        """
        self.execute("ROLLBACK TO SAVEPOINT {}".format(id))

    @contextmanager
    def disable_transactions(self):
        """
        Disable the connection's transaction support, for example by
        setting the isolation mode to 'autocommit'
        """
        self.rollback()
        yield

    def execute(self, stmt, args=tuple()):
        """
        Create a new cursor, execute a single statement and return the cursor
        object
        """
        cursor = self.cursor()
        cursor.execute(self._with_placeholders(stmt), args)
        return cursor

    def create_migrations_table(self):
        """
        Create the migrations table if it does not already exist.
        """
        sql = self.create_table_sql.format(table_name=self.migration_table)
        try:
            with self.transaction():
                self.get_applied_migration_ids()
            table_exists = True
        except self.DatabaseError:
            table_exists = False

        if not table_exists:
            with self.transaction():
                self.execute(sql)

    def _with_placeholders(self, sql):
        placeholder_gen = {'qmark': '?',
                           'format': '%s',
                           'pyformat': '%s'}.get(self.driver.paramstyle)
        if placeholder_gen is None:
            raise ValueError("Unsupported paramstyle: %r" %
                             (self.driver.paramstyle,))
        return sql.replace('?', placeholder_gen)

    def is_applied(self, migration):
        sql = self._with_placeholders(self.is_applied_sql.format(self))
        return self.execute(sql, (migration.id,)).fetchone()[0] > 0

    def get_applied_migration_ids(self):
        """
        Return the list of migration ids in the order in which they
        were applied
        """
        sql = self._with_placeholders(self.applied_ids_sql.format(self))
        return [row[0] for row in self.execute(sql).fetchall()]

    def to_apply(self, migrations):
        """
        Return the subset of migrations not already applied.
        """
        ms = (m for m in migrations if not self.is_applied(m))
        return migrations.__class__(topological_sort(ms),
                                    migrations.post_apply)

    def to_rollback(self, migrations):
        """
        Return the subset of migrations already applied and which may be
        rolled back.

        The order of migrations will be reversed.
        """
        ms = (m for m in migrations if self.is_applied(m))
        return migrations.__class__(reversed(topological_sort(ms)),
                                    migrations.post_apply)

    def apply_migrations(self, migrations, force=False):
        if migrations:
            self.apply_migrations_only(migrations, force=force)
            self.run_post_apply(migrations, force=force)

    def apply_migrations_only(self, migrations, force=False):
        """
        Apply the list of migrations, but do not run any post-apply hooks
        present.
        """
        if not migrations:
            return
        for m in migrations:
            try:
                self.apply_one(m, force=force)
            except exceptions.BadMigration:
                continue

    def run_post_apply(self, migrations, force=False):
        """
        Run any post-apply migrations present in ``migrations``
        """
        for m in migrations.post_apply:
            self.apply_one(m, mark=False, force=force)

    def rollback_migrations(self, migrations, force=False):
        if not migrations:
            return
        for m in migrations:
            try:
                self.rollback_one(m, force)
            except exceptions.BadMigration:
                continue

    def mark_migrations(self, migrations):
        with self.transaction():
            for m in migrations:
                try:
                    self.mark_one(m)
                except exceptions.BadMigration:
                    continue

    def unmark_migrations(self, migrations):
        with self.transaction():
            for m in migrations:
                try:
                    self.unmark_one(m)
                except exceptions.BadMigration:
                    continue

    def apply_one(self, migration, force=False, mark=True):
        """
        Apply a single migration
        """
        logger.info("Applying %s", migration.id)
        migration.process_steps(self, 'apply', force=force)
        if mark:
            with self.transaction():
                self.mark_one(migration)

    def rollback_one(self, migration, force=False):
        """
        Rollback a single migration
        """
        logger.info("Rolling back %s", migration.id)
        migration.process_steps(self, 'rollback', force=force)
        with self.transaction():
            self.unmark_one(migration)

    def unmark_one(self, migration):
        sql = self._with_placeholders(self.delete_migration_sql.format(self))
        self.execute(sql, (migration.id,))

    def mark_one(self, migration):
        logger.info("Marking %s applied", migration.id)
        sql = self._with_placeholders(self.insert_migration_sql).format(self)
        self.execute(sql, (migration.id, datetime.utcnow()))


class ODBCBackend(DatabaseBackend):
    driver_module = 'pyodbc'

    def connect(self, dburi):
        args = [('UID', dburi.username),
                ('PWD', dburi.password),
                ('ServerName', dburi.hostname),
                ('Port', dburi.port),
                ('Database', dburi.database)]
        args.extend(dburi.args.items())
        s = ';'.join('{}={}'.format(k, v) for k, v in args if v is not None)
        return self.driver.connect(s)


class MySQLBackend(DatabaseBackend):

    driver_module = 'pymysql'

    def connect(self, dburi):
        kwargs = dburi.args
        if dburi.username is not None:
            kwargs['user'] = dburi.username
        if dburi.password is not None:
            kwargs['passwd'] = dburi.password
        if dburi.hostname is not None:
            kwargs['host'] = dburi.hostname
        if dburi.port is not None:
            kwargs['port'] = dburi.port
        if 'unix_socket' in dburi.args:
            kwargs['unix_socket'] = dburi.args['unix_socket']
        kwargs['db'] = dburi.database

        return self.driver.connect(**kwargs)


class MySQLdbBackend(DatabaseBackend):

    driver_module = 'MySQLdb'

    def connect(self, dburi):
        kwargs = dburi.args
        if dburi.username is not None:
            kwargs['user'] = dburi.username
        if dburi.password is not None:
            kwargs['passwd'] = dburi.password
        if dburi.hostname is not None:
            kwargs['host'] = dburi.hostname
        if dburi.port is not None:
            kwargs['port'] = dburi.port
        kwargs['db'] = dburi.database

        return self.driver.connect(**kwargs)


class SQLiteBackend(DatabaseBackend):

    driver_module = 'sqlite3'
    list_tables_sql = "SELECT name FROM sqlite_master WHERE type = 'table'"

    def connect(self, dburi):
        conn = self.driver.connect(dburi.database)
        conn.isolation_level = None
        return conn


class PostgresqlBackend(DatabaseBackend):

    driver_module = 'psycopg2'

    def connect(self, dburi):
        connargs = []
        if dburi.username is not None:
            connargs.append('user=%s' % dburi.username)
        if dburi.password is not None:
            connargs.append('password=%s' % dburi.password)
        if dburi.port is not None:
            connargs.append('port=%d' % dburi.port)
        if dburi.hostname is not None:
            connargs.append('host=%s' % dburi.hostname)
        connargs.append('dbname=%s' % dburi.database)
        return self.driver.connect(' '.join(connargs))

    @contextmanager
    def disable_transactions(self):
        with super(PostgresqlBackend, self).disable_transactions():
            saved = self.connection.autocommit
            self.connection.autocommit = True
            yield
            self.connection.autocommit = saved
