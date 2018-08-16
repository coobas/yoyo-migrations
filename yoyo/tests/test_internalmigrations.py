from datetime import datetime
import getpass
import socket

from yoyo import internalmigrations
from yoyo.tests import clear_database


def assert_table_is_created(backend, table):
    assert table in backend.list_tables()


def assert_table_is_missing(backend, table):
    assert table not in backend.list_tables()


def test_it_installs_migrations_table(backend):
    clear_database(backend)
    internalmigrations.upgrade(backend)


def test_it_installs_v1(backend):
    clear_database(backend)
    internalmigrations.upgrade(backend, version=1)
    assert internalmigrations.get_current_version(backend) == 1
    assert_table_is_created(backend, '_yoyo_migration')
