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

from itertools import chain
from tempfile import mkdtemp
from textwrap import dedent
from shutil import rmtree
import os.path

dburi = "sqlite:///:memory:"


def with_migrations(*migrations, **kwmigrations):
    """
    Decorator taking a list of migrations. Creates a temporary directory writes
    each migration to a file (named '0.py', '1.py', '2.py' etc), calls the
    decorated function with the directory name as the first argument, and
    cleans up the temporary directory on exit.
    """
    def add_migrations_dir(func):
        tmpdir = mkdtemp()
        for id, code in chain(enumerate(migrations), kwmigrations.items()):
            with open(os.path.join(tmpdir, '{!s}.py'.format(id)), 'w') as f:
                f.write(dedent(code).strip())

        def decorated(*args, **kwargs):
            args = args + (tmpdir,)
            try:
                func(*args, **kwargs)
            finally:
                rmtree(tmpdir)

        return decorated

    return add_migrations_dir
