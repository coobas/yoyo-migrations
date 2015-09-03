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

from __future__ import print_function
from __future__ import unicode_literals

from datetime import date
from textwrap import dedent
from tempfile import NamedTemporaryFile
import glob
import logging
import io
import random
import shlex
import string
import subprocess
import sys
import traceback

from slugify import slugify

from yoyo import default_migration_table
from yoyo.compat import configparser
from yoyo.config import CONFIG_NEW_MIGRATION_COMMAND_KEY
from yoyo.migrations import read_migrations, heads
from yoyo import utils
from .main import InvalidArgument

from os import path, stat, unlink, rename

logger = logging.getLogger('yoyo.migrations')

migration_template = dedent('''\
    """
    {message}
    """

    from yoyo import step

    __depends__ = {{{depends}}}

    steps = [
        step("")
    ]
    ''')


def install_argparsers(global_parser, subparsers):
    parser_new = subparsers.add_parser('new',
                                        parents=[global_parser],
                                        help="Create a new migration")
    parser_new.set_defaults(func=new_migration)
    parser_new.add_argument('--message', '-m', help='Message', default='')
    parser_new.add_argument("--migration-table", dest="migration_table",
                                  action="store",
                                  default=default_migration_table,
                                  help="Name of table to use for storing "
                                  "migration metadata")
    parser_new.add_argument('sources',
                            nargs="?",
                            help="Source directory of migration scripts")
    parser_new.add_argument("database",
                            nargs="?",
                            default=None,
                            help="Database, eg 'sqlite:///path/to/sqlite.db' "
                            "or 'postgresql://user@host/db'")


def new_migration(args, config):

    sources = args.sources
    sources = sources.split()
    try:
        directory = sources[0]
    except IndexError:
        raise InvalidArgument("Please specify a migrations directory")

    message = args.message
    migrations = read_migrations(directory)
    depends = sorted(heads(migrations), key=lambda m: m.id)
    migration_source = migration_template.format(
                message=message,
                depends=', '.join('{!r}'.format(m.id) for m in depends))

    if args.batch_mode:
        p = make_filename(directory, message)
        with io.open(p, 'w', encoding='UTF-8') as f:
            f.write(migration_source)
    else:
        p = create_with_editor(config, directory, migration_source)
        if p is None:
            return

    try:
        command = config.get('DEFAULT',
                             CONFIG_NEW_MIGRATION_COMMAND_KEY)
        command = [part.format(p) for part in shlex.split(command)]
        logger.info("Running command: %s", ' '.join(command))
        subprocess.call(command)
    except configparser.NoOptionError:
        pass

    print("Created file", p)


def make_filename(directory, message):
    lines = (l.strip() for l in message.split('\n'))
    lines = (l for l in lines if l)
    message = next(lines, None)

    if message:
        slug = '-' + slugify(message)
    else:
        slug = ''

    datestr = date.today().strftime('%Y%m%d')
    number = '01'

    for p in glob.glob(path.join(directory, '{}_*'.format(datestr))):
        n = path.basename(p)[len(datestr) + 1:].split('_')[0]

        try:
            if number <= n:
                number = str(int(n) + 1).zfill(2)
        except ValueError:
            continue

    rng = random.SystemRandom()
    rand = ''.join(rng.choice(string.digits + string.ascii_letters)
                   for i in range(5))

    return path.join(directory, '{}_{}_{}{}.py'.format(
        datestr, number, rand, slug))


def create_with_editor(config, directory, migration_source):
    editor = utils.get_editor(config)
    tmpfile = NamedTemporaryFile(dir=directory, suffix='.py', delete=False)
    try:
        with io.open(tmpfile.name, 'w', encoding='UTF-8') as f:
            f.write(migration_source)

        editor = [part.format(tmpfile.name) for part in shlex.split(editor)]
        if not any(tmpfile.name in part for part in editor):
            editor.append(tmpfile.name)

        mtime = stat(tmpfile.name).st_mtime
        sys.path.insert(0, directory)
        while True:
            try:
                subprocess.call(editor)
            except OSError:
                print("Error: could not open editor!")
            else:
                if stat(tmpfile.name).st_mtime == mtime:
                    print("abort: no changes made")
                    return None

            modname = path.splitext(path.basename(tmpfile.name))[0]
            try:
                message = __import__(modname).__doc__
                break
            except Exception:
                message = ''
                print("Error loading migration")
                print(traceback.format_exc())
                print()
                r = utils.prompt("Retry editing?", "Ynq?")
                if r == 'q':
                    return None
                elif r == 'y':
                    continue
                elif r == 'n':
                    break
                elif r == '?':
                    print("")
                    print("y: reopen the migration file in your editor")
                    print("n: save the migration as-is, without re-editing")
                    print("q: quit without saving the migration")
                    print("")
                    print("?: show this help")
                    continue

        sys.path = sys.path[1:]

        filename = make_filename(directory, message)
        rename(tmpfile.name, filename)
        return filename
    finally:
        try:
            unlink(tmpfile.name)
        except OSError:
            pass
