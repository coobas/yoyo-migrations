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

from shutil import rmtree
from tempfile import mkdtemp
import os
import os.path
import sys

from mock import patch, call, Mock

from yoyo.compat import SafeConfigParser
from yoyo.tests import with_migrations, dburi
from yoyo.scripts.migrate import main, parse_args


class TestInteractiveScript(object):

    def setup(self):
        self.prompt_patch = patch('yoyo.scripts.migrate.prompt',
                                  return_value='n')
        self.prompt = self.prompt_patch.start()
        self.tmpdir = mkdtemp()
        self.saved_cwd = os.getcwd()
        os.chdir(self.tmpdir)

    def teardown(self):
        self.prompt_patch.stop()
        os.chdir(self.saved_cwd)
        rmtree(self.tmpdir)


class TestYoyoScript(TestInteractiveScript):

    @with_migrations()
    def test_it_sets_verbosity_level(self, tmpdir):
        with patch('yoyo.scripts.migrate.configure_logging') as m:
            main(['apply', tmpdir, dburi])
            assert m.call_args == call(0)
            main(['-vvv', 'apply', tmpdir, dburi])
            assert m.call_args == call(3)

    @with_migrations()
    def test_it_prompts_to_cache_connection_params(self, tmpdir):
        main(['apply', tmpdir, dburi])
        assert 'save migration config' in self.prompt.call_args[0][0].lower()

    @with_migrations()
    def test_it_caches_connection_params(self, tmpdir):
        self.prompt.return_value = 'y'
        main(['apply', tmpdir, dburi])
        assert os.path.exists('.yoyorc')
        with open('.yoyorc') as f:
            assert 'database = {0}'.format(dburi) in f.read()

    @with_migrations()
    def test_it_prompts_password(self, tmpdir):
        dburi = 'sqlite://user@/:memory'
        with patch('yoyo.scripts.migrate.getpass',
                   return_value='fish') as getpass, \
                patch('yoyo.scripts.migrate.connect',
                      return_value=(Mock(), Mock())) as connect:
            main(['apply', tmpdir, dburi, '--prompt-password'])
            assert getpass.call_count == 1
            assert connect.call_args == call('sqlite://user:fish@/:memory')

    @with_migrations()
    def test_it_prompts_migrations(self, tmpdir):
        with patch('yoyo.scripts.migrate.read_migrations') as read_migrations:
            with patch('yoyo.scripts.migrate.prompt_migrations') \
                    as prompt_migrations:
                main(['apply', tmpdir, dburi])
                migrations = read_migrations().to_apply()
                assert migrations in prompt_migrations.call_args[0]

    @with_migrations()
    def test_it_applies_migrations(self, tmpdir):
        with patch('yoyo.scripts.migrate.read_migrations') as read_migrations:
            main(['-b', 'apply', tmpdir, dburi])
            migrations = read_migrations().to_apply()
            assert migrations.rollback.call_count == 0
            assert migrations.apply.call_count == 1

    @with_migrations()
    def test_it_rollsback_migrations(self, tmpdir):
        with patch('yoyo.scripts.migrate.read_migrations') as read_migrations:
            main(['-b', 'rollback', tmpdir, dburi])
            migrations = read_migrations().to_rollback()
            assert migrations.rollback.call_count == 1
            assert migrations.apply.call_count == 0

    @with_migrations()
    def test_it_reapplies_migrations(self, tmpdir):
        with patch('yoyo.scripts.migrate.read_migrations') as read_migrations:
            main(['-b', 'reapply', tmpdir, dburi])
            migrations = read_migrations().to_rollback()
            assert migrations.rollback.call_count == 1
            assert migrations.apply.call_count == 1

    @with_migrations(m1='step("CREATE TABLE test1 (id INT)")')
    @with_migrations(m2='step("CREATE TABLE test2 (id INT)")')
    def test_it_applies_from_multiple_sources(self, t1, t2):
            with patch('yoyo.scripts.migrate.apply') as apply:
                main(['-b', 'apply', "{} {}".format(t1, t2), dburi])
                call_posargs, call_kwargs = apply.call_args
                _, migrations = call_posargs
                assert [m.path for m in migrations] == \
                        [os.path.join(t1, 'm1.py'), os.path.join(t2, 'm2.py')]


class TestArgParsing(TestInteractiveScript):

    def writeconfig(self, **defaults):
        cp = SafeConfigParser()
        for item in defaults:
            cp.set('DEFAULT', item, defaults[item])

        if sys.version_info < (3, 0):
            with open('.yoyorc', 'w') as f:
                cp.write(f)
        else:
            with open('.yoyorc', 'w', encoding='UTF-8') as f:
                cp.write(f)

    def test_it_uses_config_file_defaults(self):
        self.writeconfig(sources='/tmp/migrations',
                         database='postgresql:///foo')
        _, _, args = parse_args(['apply'])
        assert args.database == 'postgresql:///foo'
        assert args.sources == '/tmp/migrations'

    def test_cli_args_take_precendence(self):
        self.writeconfig(sources='A')
        _, _, args = parse_args(['apply', 'B', 'C'])
        assert args.sources == 'B'

    def test_global_args_can_appear_before_command(self):
        _, _, args = parse_args(['apply', 'X', 'Y'])
        assert args.verbosity == 0
        _, _, args = parse_args(['-v', 'apply', 'X', 'Y'])
        assert args.verbosity == 1

    def test_global_args_can_appear_after_command(self):
        _, _, args = parse_args(['apply', 'X', 'Y'])
        assert args.verbosity == 0
        _, _, args = parse_args(['apply', '-v', 'X', 'Y'])
        assert args.verbosity == 1
