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

import argparse
import re

from yoyo import read_migrations, default_migration_table
from yoyo.scripts.main import InvalidArgument, get_backend
from yoyo import utils


def install_argparsers(global_parser, subparsers):
    migration_parser = argparse.ArgumentParser(add_help=False)
    migration_parser.add_argument('sources',
                                  nargs="?",
                                  help="Source directory of migration scripts")

    migration_parser.add_argument("database",
                                  nargs="?",
                                  default=None,
                                  help="Database, eg 'sqlite:///path/to/sqlite.db' "
                                  "or 'postgresql://user@host/db'")

    migration_parser.add_argument("-m", "--match",
                                  help="Select migrations matching PATTERN (regular expression)",
                                  metavar='PATTERN')

    migration_parser.add_argument("-a",
                                  "--all",
                                  dest="all",
                                  action="store_true",
                                  help="Select all migrations, regardless of whether "
                                  "they have been previously applied")

    migration_parser.add_argument("-f", "--force", dest="force", action="store_true",
                                  help="Force apply/rollback of steps even if "
                                  "previous steps have failed")

    migration_parser.add_argument("-p", "--prompt-password", dest="prompt_password",
                                  action="store_true",
                                  help="Prompt for the database password")

    migration_parser.add_argument("--migration-table", dest="migration_table",
                                  action="store",
                                  default=default_migration_table,
                                  help="Name of table to use for storing "
                                  "migration metadata")

    parser_apply = subparsers.add_parser(
        'apply',
        help="Apply migrations",
        parents=[global_parser, migration_parser])
    parser_apply.set_defaults(func=apply, command_name='apply')

    parser_rollback = subparsers.add_parser(
        'rollback',
        parents=[global_parser, migration_parser],
        help="Rollback migrations")
    parser_rollback.set_defaults(func=rollback, command_name='rollback')

    parser_reapply = subparsers.add_parser(
        'reapply',
        parents=[global_parser, migration_parser],
        help="Reapply migrations")
    parser_reapply.set_defaults(func=reapply, command_name='reapply')


def get_migrations(args, backend):

    sources = args.sources
    dburi = args.database

    if sources is None:
        raise InvalidArgument("Please specify the migration source directory")

    sources = sources.split()
    migrations = read_migrations(*sources)

    if args.match:
        migrations = migrations.filter(
            lambda m: re.search(args.match, m.id) is not None)

    if not args.all:
        if args.func is apply:
            migrations = backend.to_apply(migrations)

        elif args.func in {rollback, reapply}:
            migrations = backend.to_rollback(migrations)

    if not args.batch_mode:
        migrations = prompt_migrations(backend,
                                       migrations,
                                       args.command_name)

    if not args.batch_mode and migrations:
        prompt = '{} {} to {}'.format(
            args.command_name.title(),
            utils.plural(len(migrations), " %d migration", " %d migrations"),
            dburi)
        if not utils.confirm(prompt, default='y'):
            return migrations.replace([])
    return migrations


def apply(args, config):
    backend = get_backend(args)
    migrations = get_migrations(args, backend)
    backend.apply_migrations(migrations, args.force)


def reapply(args, config):
    backend = get_backend(args)
    migrations = get_migrations(args, backend)
    backend.rollback_migrations(migrations, args.force)
    backend.apply_migrations(migrations, args.force)


def rollback(args, config):
    backend = get_backend(args)
    migrations = get_migrations(args, backend)
    backend.rollback_migrations(migrations, args.force)


def prompt_migrations(backend, migrations, direction):
    """
    Iterate through the list of migrations and prompt the user to
    apply/rollback each. Return a list of user selected migrations.

    direction
        one of 'apply' or 'rollback'
    """
    class prompted_migration(object):

        def __init__(self, migration, default=None):
            super(prompted_migration, self).__init__()
            self.migration = migration
            self.choice = default

    migrations = migrations.replace(prompted_migration(m) for m in migrations)

    position = 0
    while position < len(migrations):
        mig = migrations[position]

        choice = mig.choice
        if choice is None:
            isapplied = backend.isapplied(mig.migration)
            if direction == 'apply':
                choice = 'n' if isapplied else 'y'
            else:
                choice = 'y' if isapplied else 'n'
        options = ''.join(o.upper() if o == choice else o.lower()
                          for o in 'ynvdaqjk?')

        print("")
        print('[%s]' % (mig.migration.id,))
        response = utils.prompt("Shall I %s this migration?" % (direction,),
                                options)

        if response == '?':
            print("")
            print("y: %s this migration" % (direction,))
            print("n: don't %s it" % (direction,))
            print("")
            print("v: view this migration in full")
            print("")
            print("d: %s the selected migrations, skipping any remaining" %
                    (direction,))
            print("a: %s all the remaining migrations" % (direction,))
            print("q: cancel without making any changes")
            print("")
            print("j: skip to next migration")
            print("k: back up to previous migration")
            print("")
            print("?: show this help")
            continue

        if response in 'yn':
            mig.choice = response
            position += 1
            continue

        if response == 'v':
            print(mig.migration.source)
            continue

        if response == 'j':
            position = min(len(migrations), position + 1)
            continue

        if response == 'k':
            position = max(0, position - 1)

        if response == 'd':
            break

        if response == 'a':
            for mig in migrations[position:]:
                mig.choice = 'y'
            break

        if response == 'q':
            for mig in migrations:
                mig.choice = 'n'
            break

    return migrations.replace(m.migration
                              for m in migrations
                              if m.choice == 'y')
