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

import os
import time

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
    lock_table = 'yoyo_lock'
    create_migration_table_sql = """
        CREATE TABLE {table_name_quoted} (
            id VARCHAR(255) NOT NULL PRIMARY KEY,
            ctime TIMESTAMP
        )"""
    create_lock_table_sql = """
        CREATE TABLE {table_name_quoted} (
            locked INT DEFAULT 1,
            ctime TIMESTAMP,
            pid INT NOT NULL,
            PRIMARY KEY (locked)
        )"""
    list_tables_sql = "SELECT table_name FROM information_schema.tables"
    is_applied_sql = """
        SELECT COUNT(1) FROM {0.migration_table_quoted}
        WHERE id=?"""
    insert_migration_sql = """
        INSERT INTO {0.migration_table_quoted} (id, ctime)
        VALUES (?, ?)"""
    delete_migration_sql = "DELETE FROM {0.migration_table_quoted} WHERE id=?"
    applied_ids_sql = "SELECT id FROM {0.migration_table_quoted} ORDER by ctime"
    create_test_table_sql = """
        CREATE TABLE {table_name_quoted}
        (id INT PRIMARY KEY)"""

    _driver = None
    _in_transaction = False

    def __init__(self, dburi, migration_table):
        self.uri = dburi
        self.DatabaseError = self.driver.DatabaseError
        self._connection = self.connect(dburi)
        self.init_connection(self._connection)
        self.migration_table = migration_table
        self.create_tables()
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

    def init_connection(self, connection):
        """
        Called when creating a connection or after a rollback. May do any
        db specific tasks required to make the connection ready for use.
        """

    @property
    def migration_table_quoted(self):
        return self.quote_identifier(self.migration_table)

    @property
    def lock_table_quoted(self):
        return self.quote_identifier(self.lock_table)

    def quote_identifier(self, s):
        return '"{}"'.format(s)

    def _check_transactional_ddl(self):
        """
        Return True if the database supports committing/rolling back
        DDL statements within a transaction
        """
        table_name = 'yoyo_tmp_{}'.format(utils.get_random_string(10))
        table_name_quoted = self.quote_identifier(table_name)
        sql = self.create_test_table_sql.format(
            table_name_quoted=table_name_quoted)
        with self.transaction() as t:
            self.execute(sql)
            t.rollback()
        try:
            with self.transaction():
                self.execute("DROP TABLE {}".format(table_name_quoted))
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
        self.init_connection(self.connection)
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

    @contextmanager
    def lock(self, timeout=10):
        """
        Create a lock to prevent concurrent migrations.

        :param timeout: duration in seconds before raising a LockTimeout error.
        """

        pid = os.getpid()
        self._insert_lock_row(pid, timeout)
        try:
            yield
        finally:
            self._delete_lock_row(pid)

    def _insert_lock_row(self, pid, timeout, poll_interval=0.5):
        started = time.time()
        while True:
            try:
                with self.transaction():
                    self.execute("INSERT INTO {} (locked, ctime, pid) "
                                 "VALUES (1, ?, ?)".format(self.lock_table_quoted),
                                 (datetime.utcnow(), pid))
            except self.DatabaseError:
                if timeout and time.time() > started + timeout:
                    cursor = self.execute("SELECT pid FROM {}"
                                        .format(self.lock_table_quoted))
                    row = cursor.fetchone()
                    if row:
                        raise exceptions.LockTimeout(
                            "Process {} has locked this database "
                            "(run yoyo break-lock to remove this lock)"
                            .format(row[0]))
                    else:
                        raise exceptions.LockTimeout(
                            "Database locked "
                            "(run yoyo break-lock to remove this lock)")
                time.sleep(poll_interval)
            else:
                return

    def _delete_lock_row(self, pid):
        with self.transaction():
            self.execute("DELETE FROM {} WHERE pid=?"
                         .format(self.lock_table_quoted),
                         (pid,))

    def break_lock(self):
        with self.transaction():
            self.execute("DELETE FROM {}" .format(self.lock_table_quoted))

    def execute(self, stmt, args=tuple()):
        """
        Create a new cursor, execute a single statement and return the cursor
        object
        """
        cursor = self.cursor()
        cursor.execute(self._with_placeholders(stmt), args)
        return cursor

    def create_tables(self):
        """
        Create the migrations and lock tables if they do not already exist.
        """
        statements = [
            self.create_migration_table_sql.format(
                table_name_quoted=self.migration_table_quoted),
            self.create_lock_table_sql.format(
                table_name_quoted=self.lock_table_quoted)
        ]

        for stmt in statements:
            try:
                with self.transaction():
                    self.execute(stmt)
            except self.DatabaseError:
                pass

    def _with_placeholders(self, sql):
        placeholder_gen = {'qmark': '?',
                           'named': ':s',
                           'format': '%s',
                           'pyformat': '%s'}.get(self.driver.paramstyle)
        if placeholder_gen is None:
            raise ValueError("Unsupported paramstyle: %r" %
                             (self.driver.paramstyle,))
        return sql.replace('?', placeholder_gen)

    def is_applied(self, migration):
        sql = self.is_applied_sql.format(self)
        return self.execute(sql, (migration.id,)).fetchone()[0] > 0

    def get_applied_migration_ids(self):
        """
        Return the list of migration ids in the order in which they
        were applied
        """
        sql = self.applied_ids_sql.format(self)
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
        sql = self.delete_migration_sql.format(self)
        self.execute(sql, (migration.id,))

    def mark_one(self, migration):
        logger.info("Marking %s applied", migration.id)
        sql = self.insert_migration_sql.format(self)
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


class OracleBackend(DatabaseBackend):

    driver_module = 'cx_Oracle'
    list_tables_sql = 'SELECT table_name FROM all_tables WHERE owner=user'

    def begin(self):
        """Oracle is always in a transaction, and has no "BEGIN" statement."""
        self._in_transaction = True

    def connect(self, dburi):
        kwargs = dburi.args
        if dburi.username is not None:
            kwargs['user'] = dburi.username
        if dburi.password is not None:
            kwargs['password'] = dburi.password
        # Oracle combines the hostname, port and database into a single DSN.
        # The DSN can also be a "net service name"
        kwargs['dsn'] = ''
        if dburi.hostname is not None:
            kwargs['dsn'] = dburi.hostname
        if dburi.port is not None:
            kwargs['dsn'] += ':{0}'.format(dburi.port)
        if dburi.database is not None:
            if kwargs['dsn']:
                kwargs['dsn'] += '/{0}'.format(dburi.database)
            else:
                kwargs['dsn'] = dburi.database

        return self.driver.connect(**kwargs)


class MySQLBackend(DatabaseBackend):

    driver_module = 'pymysql'

    def connect(self, dburi):
        kwargs = {'db': dburi.database}
        kwargs.update(dburi.args)
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

    def quote_identifier(self, identifier):
        sql_mode = self.execute("SHOW VARIABLES LIKE 'sql_mode'").fetchone()[1]
        if 'ansi_quotes' in sql_mode.lower():
            return super(MySQLBackend).quote_identifier(identifier)
        return "`{}`".format(identifier)


class MySQLdbBackend(MySQLBackend):
    driver_module = 'MySQLdb'


class SQLiteBackend(DatabaseBackend):

    driver_module = 'sqlite3'
    list_tables_sql = "SELECT name FROM sqlite_master WHERE type = 'table'"

    def connect(self, dburi):
        conn = self.driver.connect(dburi.database)
        conn.isolation_level = None
        return conn


class PostgresqlBackend(DatabaseBackend):

    driver_module = 'psycopg2'
    schema = None

    def connect(self, dburi):
        kwargs = {'dbname': dburi.database}
        kwargs.update(dburi.args)
        if dburi.username is not None:
            kwargs['user'] = dburi.username
        if dburi.password is not None:
            kwargs['password'] = dburi.password
        if dburi.port is not None:
            kwargs['port'] = dburi.port
        if dburi.hostname is not None:
            kwargs['host'] = dburi.hostname
        self.schema = kwargs.pop('schema', None)
        return self.driver.connect(**kwargs)

    @contextmanager
    def disable_transactions(self):
        with super(PostgresqlBackend, self).disable_transactions():
            saved = self.connection.autocommit
            self.connection.autocommit = True
            yield
            self.connection.autocommit = saved

    def init_connection(self, connection):
        if self.schema:
            cursor = connection.cursor()
            cursor.execute("SET search_path TO {}".format(self.schema))
