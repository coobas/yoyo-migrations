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

import pytest
from mock import Mock

from yoyo.connections import get_backend
from yoyo import read_migrations
from yoyo import exceptions
from yoyo import ancestors, descendants

from yoyo.tests import with_migrations, dburi
from yoyo.migrations import topological_sort, MigrationList


@with_migrations(
    """
    step("CREATE TABLE _yoyo_test (id INT)")
    """,
    """
step("INSERT INTO _yoyo_test VALUES (1)")
step("INSERT INTO _yoyo_test VALUES ('x', 'y')")
    """)
def test_transaction_is_not_committed_on_error(tmpdir):
    backend = get_backend(dburi)
    migrations = read_migrations(tmpdir)
    with pytest.raises(backend.DatabaseError):
        backend.apply_migrations(migrations)
    cursor = backend.cursor()
    cursor.execute("SELECT count(1) FROM _yoyo_test")
    assert cursor.fetchone() == (0,)


@with_migrations(
    'step("CREATE TABLE _yoyo_test (id INT)")',
    '''
step("INSERT INTO _yoyo_test VALUES (1)", "DELETE FROM _yoyo_test WHERE id=1")
step("UPDATE _yoyo_test SET id=2 WHERE id=1", "UPDATE _yoyo_test SET id=1 WHERE id=2")
    '''
)
def test_rollbacks_happen_in_reverse(tmpdir):
    backend = get_backend(dburi)
    migrations = read_migrations(tmpdir)
    backend.apply_migrations(migrations)
    cursor = backend.cursor()
    cursor.execute("SELECT * FROM _yoyo_test")
    assert cursor.fetchall() == [(2,)]
    backend.rollback_migrations(migrations)
    cursor.execute("SELECT * FROM _yoyo_test")
    assert cursor.fetchall() == []


@with_migrations(
    '''
    step("CREATE TABLE _yoyo_test (id INT)")
    step("INSERT INTO _yoyo_test VALUES (1)")
    step("INSERT INTO _yoyo_test VALUES ('a', 'b')", ignore_errors='all')
    step("INSERT INTO _yoyo_test VALUES (2)")
    '''
)
def test_execution_continues_with_ignore_errors(tmpdir):
    backend = get_backend(dburi)
    migrations = read_migrations(tmpdir)
    backend.apply_migrations(migrations)
    cursor = backend.cursor()
    cursor.execute("SELECT * FROM _yoyo_test")
    assert cursor.fetchall() == [(1,), (2,)]


@with_migrations(
    '''
    from yoyo import step, group
    step("CREATE TABLE _yoyo_test (id INT)")
    group(
        step("INSERT INTO _yoyo_test VALUES (1)"),
        step("INSERT INTO _yoyo_test VALUES ('a', 'b')"),
        ignore_errors='all'
    )
    step("INSERT INTO _yoyo_test VALUES (2)")
    '''
)
def test_execution_continues_with_ignore_errors_in_transaction(tmpdir):
    backend = get_backend(dburi)
    migrations = read_migrations(tmpdir)
    backend.apply_migrations(migrations)
    cursor = backend.cursor()
    cursor.execute("SELECT * FROM _yoyo_test")
    assert cursor.fetchall() == [(2,)]


@with_migrations(
    '''
    step("CREATE TABLE _yoyo_test (id INT)")
    step("INSERT INTO _yoyo_test VALUES (1)",
         "DELETE FROM _yoyo_test WHERE id=2")
    step("UPDATE _yoyo_test SET id=2 WHERE id=1",
         "SELECT nonexistent FROM imaginary", ignore_errors='rollback')
    '''
)
def test_rollbackignores_errors(tmpdir):
    backend = get_backend(dburi)
    migrations = read_migrations(tmpdir)
    backend.apply_migrations(migrations)
    cursor = backend.cursor()
    cursor.execute("SELECT * FROM _yoyo_test")
    assert cursor.fetchall() == [(2,)]

    backend.rollback_migrations(migrations)
    cursor.execute("SELECT * FROM _yoyo_test")
    assert cursor.fetchall() == []


@with_migrations(
    '''
    step("CREATE TABLE _yoyo_test (id INT)")
    step("DROP TABLE _yoyo_test")
    '''
)
def test_specify_migration_table(tmpdir):
    backend = get_backend(dburi, migration_table='another_migration_table')
    migrations = read_migrations(tmpdir)
    backend.apply_migrations(migrations)
    cursor = backend.cursor()
    cursor.execute("SELECT id FROM another_migration_table")
    assert cursor.fetchall() == [('0',)]


@with_migrations(
    '''
    def foo(conn):
        conn.cursor().execute("CREATE TABLE foo_test (id INT)")
        conn.cursor().execute("INSERT INTO foo_test VALUES (1)")
    def bar(conn):
        foo(conn)
    step(bar)
    '''
)
def test_migration_functions_have_namespace_access(tmpdir):
    """
    Test that functions called via step have access to the script namespace
    """
    backend = get_backend(dburi)
    migrations = read_migrations(tmpdir)
    backend.apply_migrations(migrations)
    cursor = backend.cursor()
    cursor.execute("SELECT id FROM foo_test")
    assert cursor.fetchall() == [(1,)]


@with_migrations(
    '''
    from yoyo import group, step
    step("CREATE TABLE _yoyo_test (id INT)")
    group(step("INSERT INTO _yoyo_test VALUES (1)")),
    '''
)
def test_migrations_can_import_step_and_group(tmpdir):
    backend = get_backend(dburi)
    migrations = read_migrations(tmpdir)
    backend.apply_migrations(migrations)
    cursor = backend.cursor()
    cursor.execute("SELECT id FROM _yoyo_test")
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


class TestMigrationList(object):

    def test_cannot_create_with_duplicate_ids(self):
        with pytest.raises(exceptions.MigrationConflict):
            MigrationList([Mock(id=1), Mock(id=1)])

    def test_can_append_new_id(self):
        m = MigrationList([Mock(id=n) for n in range(10)])
        m.append(Mock(id=10))

    def test_cannot_append_duplicate_id(self):
        m = MigrationList([Mock(id=n) for n in range(10)])
        with pytest.raises(exceptions.MigrationConflict):
            m.append(Mock(id=1))

    def test_deletion_allows_reinsertion(self):
        m = MigrationList([Mock(id=n) for n in range(10)])
        del m[0]
        m.append(Mock(id=0))

    def test_can_overwrite_slice_with_same_ids(self):
        m = MigrationList([Mock(id=n) for n in range(10)])
        m[1:3] = [Mock(id=2), Mock(id=1)]

    def test_cannot_overwrite_slice_with_conflicting_ids(self):
        m = MigrationList([Mock(id=n) for n in range(10)])
        with pytest.raises(exceptions.MigrationConflict):
            m[1:3] = [Mock(id=4)]


class TestAncestorsDescendants(object):
    def setup(self):
        self.m1 = Mock(id='m1', depends=['m2', 'm3'])
        self.m2 = Mock(id='m2', depends=['m3'])
        self.m3 = Mock(id='m3', depends=['m5'])
        self.m4 = Mock(id='m4', depends=['m5'])
        self.m5 = Mock(id='m5', depends=[])
        self.m1.depends = {self.m2, self.m3}
        self.m2.depends = {self.m3}
        self.m3.depends = {self.m5}
        self.m4.depends = {self.m5}
        self.migrations = {self.m1, self.m2, self.m3, self.m4, self.m5}

    def test_ancestors(self):

        assert ancestors(self.m1, self.migrations) == {self.m2, self.m3,
                                                       self.m5}
        assert ancestors(self.m2, self.migrations) == {self.m3, self.m5}
        assert ancestors(self.m3, self.migrations) == {self.m5}
        assert ancestors(self.m4, self.migrations) == {self.m5}
        assert ancestors(self.m5, self.migrations) == set()

    def test_descendants(self):

        assert descendants(self.m1, self.migrations) == set()
        assert descendants(self.m2, self.migrations) == {self.m1}
        assert descendants(self.m3, self.migrations) == {self.m2, self.m1}
        assert descendants(self.m4, self.migrations) == set()
        assert descendants(self.m5, self.migrations) == {self.m4, self.m3,
                                                         self.m2, self.m1}
