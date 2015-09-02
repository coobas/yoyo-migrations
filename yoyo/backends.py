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
from importlib import import_module
from logging import getLogger
import contextlib

from . import exceptions
from .migrations import topological_sort

logger = getLogger('yoyo.migrations')


class DatabaseBackend(object):

    driver_module = None
    connection = None
    create_table_sql = """
        CREATE TABLE {table_name} (
            id VARCHAR(255) NOT NULL PRIMARY KEY,
            ctime TIMESTAMP
        )"""
    is_applied_sql = "SELECT COUNT(1) FROM {0.migration_table} WHERE id=?"
    insert_migration_sql = ("INSERT INTO {0.migration_table} (id, ctime) "
                            "VALUES (?, ?)")
    delete_migration_sql = "DELETE FROM {0.migration_table} WHERE id=?"
    applied_ids_sql = "SELECT id FROM {0.migration_table} ORDER by ctime"

    _driver = None

    def __init__(self, dburi, migration_table):
        self.uri = dburi
        self.connection = self.connect(dburi)
        self.migration_table = migration_table
        self.create_migrations_table()

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

    @contextlib.contextmanager
    def transaction(self):
        try:
            yield
        except:
            self.connection.rollback()
        else:
            self.connection.commit()

    def cursor(self):
        return self.connection.cursor()

    def commit(self):
        return self.connection.commit()

    def rollback(self):
        return self.connection.rollback()

    def create_migrations_table(self):
        """
        Create the migrations table if it does not already exist.
        """
        sql = self.create_table_sql.format(table_name=self.migration_table)
        try:
            cursor = self.connection.cursor()
            try:
                cursor.execute(sql)
                self.connection.commit()
            except tuple(exceptions.DatabaseErrors):
                pass
            finally:
                cursor.close()
        finally:
            self.connection.rollback()

    def _with_placeholders(self, sql):
        placeholder_gen = {'qmark': '?',
                           'format': '%s',
                           'pyformat': '%s'}.get(self.driver.paramstyle)
        if placeholder_gen is None:
            raise ValueError("Unsupported paramstyle: %r" %
                             (self.driver.paramstyle,))
        return sql.replace('?', placeholder_gen)

    def is_applied(self, migration):
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                self._with_placeholders(self.is_applied_sql.format(self)),
                (migration.id,))
            return cursor.fetchone()[0] > 0
        finally:
            cursor.close()

    def get_applied_migration_ids(self):
        """
        Return the list of migration ids in the order in which they
        were applied
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                self._with_placeholders(self.applied_ids_sql.format(self)))
            return [row[0] for row in cursor.fetchall()]
        finally:
            cursor.close()

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
        if not migrations:
            return
        for m in migrations + migrations.post_apply:
            try:
                self.apply_one(m, force)
            except exceptions.BadMigration:
                continue

    def rollback_migrations(self, migrations, force=False):
        if not migrations:
            return
        for m in migrations + migrations.post_apply:
            try:
                self.rollback_one(m, force)
            except exceptions.BadMigration:
                continue

    def mark_migrations(self, migrations):
        for m in migrations:
            try:
                self.mark_one(m)
            except exceptions.BadMigration:
                continue

    def apply_one(self, migration, force=False):
        logger.info("Applying %s", migration.id)
        migration.process_steps(self, 'apply', force=force)
        self.mark_one(migration)

    def rollback_one(self, migration, force=False):
        logger.info("Rolling back %s", migration.id)
        migration.process_steps(self, 'rollback', force=force)
        cursor = self.connection.cursor()
        cursor.execute(
            self._with_placeholders(self.delete_migration_sql.format(self)),
            (migration.id,))
        self.connection.commit()
        cursor.close()

    def mark_one(self, migration):
        logger.info("Marking %s applied", migration.id)
        cursor = self.connection.cursor()
        cursor.execute(
            self._with_placeholders(self.insert_migration_sql).format(self),
            (migration.id, datetime.utcnow()))
        self.connection.commit()
        cursor.close()


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

    def connect(self, dburi):
        return self.driver.connect(dburi.database)


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
