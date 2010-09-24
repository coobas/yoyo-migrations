import re
from fabric.api import *

env.shell = '/bin/sh -c'

def release():

    local("test \! -e clean")

    with open("VERSION.txt", 'r') as f:
        version = f.read().strip()

    assert version.endswith('dev')
    release_version = version.replace('dev', '')
    new_dev_version = increment_version(release_version) + 'dev'

    with open("CHANGELOG.txt", 'r') as f:
        changes = f.read()

    # Check we've a changelog entry for the newly released version
    assert re.search(
        r'\b%s\b' % (re.escape(release_version),),
        changes,
        re.M
    ) is not None, "No changelog entry found for version %s" % (release_version,)

    # Bump to a release version number
    with open("VERSION.txt", 'w') as f:
        f.write(release_version + '\n')

    local("darcs record -a VERSION.txt -m 'Bumped version number'")
    local("darcs tag %s" % (release_version,))
    local("darcs get --lazy --ephemeral . clean")
    local("cd clean && python bootstrap.py")
    local("cd clean && ./bin/buildout")
    local("cd clean && ./bin/python setup.py sdist")
    check_release()
    local("cd clean && ./bin/python setup.py sdist upload")
    local("rm -rf clean")

    with open("VERSION.txt", 'w') as f:
        f.write(
            prompt(
                "New development version number?",
                default=increment_version(release_version) + 'dev'
            ) + '\n'
        )

    local("darcs record -a VERSION.txt -m 'Bumped version number'")


def check_release():
    """
    Check that the sdist can be installed and imported
    """
    local("test \! -e test_virtualenv")
    local("virtualenv test_virtualenv")
    local("./test_virtualenv/bin/easy_install ./clean/dist/yoyo-migrations*.tar.gz")
    local("./test_virtualenv/bin/python -c'import yoyo.migrate'")
    local("rm -rf test_virtualenv")

def increment_version(version):
    """
    Increment the least significant part of a version number string.

        >>> increment_version("1.0.0")
        '1.0.1'
    """

    version = map(int, version.split('.'))
    version = version[:-1] + [version[-1] + 1,]
    version = '.'.join(map(str, version))
    return version
