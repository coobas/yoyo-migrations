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

from __future__ import unicode_literals

from shutil import rmtree
from tempfile import mkdtemp
import io
import os
import os.path
import sys

from mock import patch, call

from yoyo import read_migrations
from yoyo.compat import SafeConfigParser
from yoyo.tests import with_migrations, dburi
from yoyo.scripts.main import main, parse_args, LEGACY_CONFIG_FILENAME


class TestInteractiveScript(object):

    def setup(self):
        self.confirm_patch = patch('yoyo.utils.confirm', return_value=False)
        self.confirm = self.confirm_patch.start()
        self.prompt_patch = patch('yoyo.utils.prompt', return_value='n')
        self.prompt = self.prompt_patch.start()
        self.tmpdir = mkdtemp()
        self.dburi = 'sqlite:////' + self.tmpdir + 'db.sqlite'
        self.saved_cwd = os.getcwd()
        os.chdir(self.tmpdir)

    def teardown(self):
        self.prompt_patch.stop()
        self.confirm_patch.stop()
        os.chdir(self.saved_cwd)
        rmtree(self.tmpdir)

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


class TestYoyoScript(TestInteractiveScript):

    @with_migrations()
    def test_it_sets_verbosity_level(self, tmpdir):
        with patch('yoyo.scripts.main.configure_logging') as m:
            main(['apply', tmpdir, dburi])
            assert m.call_args == call(0)
            main(['-vvv', 'apply', tmpdir, dburi])
            assert m.call_args == call(3)

    @with_migrations()
    def test_it_prompts_to_create_config_file(self, tmpdir):
        main(['apply', tmpdir, dburi])
        assert 'save migration config' in self.confirm.call_args[0][0].lower()

    @with_migrations()
    def test_it_creates_config_file(self, tmpdir):
        self.confirm.return_value = True
        main(['apply', tmpdir, dburi])
        assert os.path.exists('.yoyorc')
        with open('.yoyorc') as f:
            assert 'database = {0}'.format(dburi) in f.read()

    @with_migrations()
    def test_it_uses_config_file(self, tmpdir):
        self.writeconfig(batch_mode='on')
        with patch('yoyo.scripts.migrate.apply') as apply:
            main(['apply', tmpdir, dburi])
            args_used = apply.call_args[0][0]
            assert args_used.batch_mode is True

    @with_migrations()
    def test_it_ignores_config_file(self, tmpdir):
        self.writeconfig(batch_mode='on')
        with patch('yoyo.scripts.migrate.apply') as apply:
            main(['apply', '--no-config-file', tmpdir, dburi])
            args_used = apply.call_args[0][0]
            assert args_used.batch_mode is False

    @with_migrations()
    def test_it_prompts_password(self, tmpdir):
        dburi = 'sqlite://user@/:memory'
        with patch('yoyo.scripts.main.getpass',
                   return_value='fish') as getpass, \
                patch('yoyo.connections.get_backend') as get_backend:
            main(['apply', tmpdir, dburi, '--prompt-password'])
            assert getpass.call_count == 1
            assert get_backend.call_args == call('sqlite://user:fish@/:memory',
                                                 '_yoyo_migration')

    @with_migrations()
    def test_it_prompts_migrations(self, tmpdir):
        with patch('yoyo.scripts.migrate.prompt_migrations') \
                as prompt_migrations, \
                patch('yoyo.scripts.migrate.get_backend') as get_backend:
            main(['apply', tmpdir, dburi])
            migrations = get_backend().to_apply()
            assert migrations in prompt_migrations.call_args[0]

    @with_migrations()
    def test_it_applies_migrations(self, tmpdir):
        with patch('yoyo.scripts.migrate.get_backend') as get_backend:
            main(['-b', 'apply', tmpdir, dburi])
            assert get_backend().rollback_migrations.call_count == 0
            assert get_backend().apply_migrations.call_count == 1

    @with_migrations()
    def test_it_rollsback_migrations(self, tmpdir):
        with patch('yoyo.scripts.migrate.get_backend') as get_backend:
            main(['-b', 'rollback', tmpdir, dburi])
            assert get_backend().rollback_migrations.call_count == 1
            assert get_backend().apply_migrations.call_count == 0

    @with_migrations()
    def test_it_reapplies_migrations(self, tmpdir):
        with patch('yoyo.scripts.migrate.get_backend') as get_backend:
            main(['-b', 'reapply', tmpdir, dburi])
            assert get_backend().rollback_migrations.call_count == 1
            assert get_backend().apply_migrations.call_count == 1

    @with_migrations(m1='step("CREATE TABLE test1 (id INT)")')
    @with_migrations(m2='step("CREATE TABLE test2 (id INT)")')
    def test_it_applies_from_multiple_sources(self, t1, t2):
            with patch('yoyo.backends.DatabaseBackend.apply_migrations') \
                    as apply:
                main(['-b', 'apply', "{} {}".format(t1, t2), dburi])
                call_posargs, call_kwargs = apply.call_args
                migrations, _ = call_posargs
                assert [m.path for m in migrations] == \
                        [os.path.join(t1, 'm1.py'), os.path.join(t2, 'm2.py')]

    @with_migrations()
    def test_it_offers_to_upgrade(self, tmpdir):
        legacy_config_path = os.path.join(tmpdir, LEGACY_CONFIG_FILENAME)
        with io.open(legacy_config_path, 'w', encoding='utf-8') as f:
            f.write('[DEFAULT]\n')
            f.write('migration_table=_yoyo_migration\n')
            f.write('dburi=sqlite:///\n')

        self.confirm.return_value = True
        main(['apply', tmpdir])
        prompts = [args[0].lower()
                    for args, kwargs in self.confirm.call_args_list]
        assert prompts[0].startswith('move legacy configuration')
        assert prompts[1].startswith('delete legacy configuration')
        assert not os.path.exists(legacy_config_path)

        with open('.yoyorc', 'r') as f:
            config = f.read()
            assert 'database = sqlite:///\n' in config
            assert 'migration_table = _yoyo_migration\n' in config

    @with_migrations()
    def test_it_upgrades_migration_table_None(self, tmpdir):
        legacy_config_path = os.path.join(tmpdir, LEGACY_CONFIG_FILENAME)
        with io.open(legacy_config_path, 'w', encoding='utf-8') as f:
            f.write('[DEFAULT]\n')
            f.write('migration_table=None\n')
            f.write('dburi=sqlite:///\n')
        self.confirm.return_value = True
        main(['apply', tmpdir])

        with open('.yoyorc', 'r') as f:
            config = f.read()
        assert 'migration_table = _yoyo_migration\n' in config


class TestArgParsing(TestInteractiveScript):

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


class TestMarkCommand(TestInteractiveScript):

    @with_migrations(m1='step("CREATE TABLE test1 (id INT)")',
                     m2='step("CREATE TABLE test2 (id INT)")',
                     m3='step("CREATE TABLE test3 (id INT)")')
    def test_it_prompts_only_unapplied(self, tmpdir):
        from yoyo.connections import get_backend
        migrations = read_migrations(tmpdir)
        backend = get_backend(self.dburi)
        backend.apply_migrations(migrations[:1])
        backend.commit()

        with patch('yoyo.scripts.migrate.prompt_migrations') \
                as prompt_migrations:
            main(['mark', tmpdir, self.dburi])
            _, prompted, _ = prompt_migrations.call_args[0]
            prompted = [m.id for m in prompted]
            assert prompted == ['m2', 'm3']

    @with_migrations(m1='step("INSERT INTO t VALUES (1)")',
                     m2='__depends__=["m1"]; step("INSERT INTO t VALUES (2)")',
                     m3='step("INSERT INTO t VALUES (2)")')
    def test_it_marks_at_selected_version(self, tmpdir):
        from yoyo.connections import get_backend
        self.confirm.return_value = True
        migrations = read_migrations(tmpdir)
        backend = get_backend(self.dburi)
        backend.cursor().execute("CREATE TABLE t (id INT)")
        backend.commit()

        main(['mark', '-r', 'm2', tmpdir, self.dburi])
        assert backend.is_applied(migrations[0])
        assert backend.is_applied(migrations[1])
        assert not backend.is_applied(migrations[2])

        # Check that migration steps have not been applied
        c = backend.cursor()
        c.execute("SELECT * FROM t")
        assert len(c.fetchall()) == 0
