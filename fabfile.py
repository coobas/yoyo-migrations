import re
from fabric.api import *

env.shell = '/bin/sh -c'
env.package_name = 'yoyo-migrations'
env.module_name = 'yoyo.migrate'

# Where to host generated sphinx documentation
env.hosts = ['www.ollycope.com']
env.docsdir = 'www/ollycope.com/htdocs/software/%(package_name)s' % env

# Where to locally checkout a clean version for build and upload
env.builddir = './clean'

def builddocs():
    """
    Run doctests and build HTML docs
    """
    local("cd %(builddir)s/doc && make doctest clean html" % env)

def uploaddocs():
    """
    Upload sphinx docs to website
    """
    env.version = local("python %(builddir)s/setup.py --version" % env, capture=True).strip()
    local("tar -cf docs.tar -C %(builddir)s/doc/_build/html ." % env)
    run("test -d %(docsdir)s/%(version)s || mkdir -p %(docsdir)s/%(version)s" % env)
    put("docs.tar", "%(docsdir)s/%(version)s/" % env)
    run("cd %(docsdir)s/%(version)s && tar xf docs.tar" % env)
    run("cd %(docsdir)s/%(version)s && rm docs.tar" % env)

    run("cd %(docsdir)s; test -L latest && rm latest || true" % env)
    run("cd %(docsdir)s; ln -s %(version)s latest" % env)

def _make_clean_checkout():
    local("rm -rf %(builddir)s" % env)
    local("darcs get --lazy . %(builddir)s" % env)

def _make_build():
    local("cd %(builddir)s && python bootstrap.py" % env)
    local("cd %(builddir)s && ./bin/buildout" % env)
    local("cd %(builddir)s && ./bin/python setup.py sdist" % env)

def build(clean="yes"):
    """
    Checkout and build a clean source distribution
    """
    _make_clean_checkout()
    _make_build()
    _check_release()
    #builddocs()

def _readversion():
    """
    Read the contents of VERSION.txt and return the current version number
    """
    with open("%(builddir)s/VERSION.txt" % env, 'r') as f:
        return f.read().strip()

def _check_changelog(version):
    """
    Check that a changelog entry exists for the given version
    """

    with open("%(builddir)s/CHANGELOG.txt" % env, 'r') as f:
        changes = f.read()

    # Check we've a changelog entry for the newly released version
    assert re.search(
        r'\b%s\b' % (re.escape(version),),
        changes,
        re.M
    ) is not None, "No changelog entry found for version %s" % (version,)

def _updateversion(version):
    """
    Write the given version number to VERSION.txt and record a new patch
    """
    _set_darcs_author()
    with open("%(builddir)s/VERSION.txt" % env, 'w') as f:
        f.write(version + '\n')
    local("darcs record -A %(darcs_author)s --repodir=%(builddir)s -a VERSION.txt -m 'Bumped version number'" % env)

def _tag(version):
    _set_darcs_author()
    local("darcs tag %s --repodir=%s -A %s" % (version, env.builddir, env.darcs_author))

def release():
    """
    Upload a new release to the PyPI. Requires ``build`` to have been run previously.
    """

    version = _readversion()
    assert version.endswith('dev')
    release_version = version.replace('dev', '')
    _check_changelog(release_version)

    _updateversion(release_version)
    _tag(release_version)

    #uploaddocs()
    local("cd %(builddir)s && ./bin/python setup.py sdist upload" % env)

    _updateversion(
        prompt(
            "New development version number?",
            default=_increment_version(release_version) + 'dev'
        ) + '\n'
    )
    local("darcs pull --no-set-default %(builddir)s" % env, capture=False)

def _check_release():
    """
    Check that the sdist can be at least be installed and imported
    """
    local("cd %(builddir)s && ./bin/nosetests" % env)
    local("test \! -e test_virtualenv")
    local("virtualenv test_virtualenv")
    local("./test_virtualenv/bin/easy_install ./%(builddir)s/dist/*.tar.gz" % env)
    local("./test_virtualenv/bin/python -c'import %(module_name)s'" % env)
    local("rm -rf test_virtualenv")

def _increment_version(version):
    """
    Increment the least significant part of a version number string.

        >>> _increment_version("1.0.0")
        '1.0.1'
    """
    version = map(int, version.split('.'))
    version = version[:-1] + [version[-1] + 1,]
    version = '.'.join(map(str, version))
    return version

def _set_darcs_author():
    if env.get("darcs_author"):
        return env.get("darcs_author")
    env.darcs_author = re.search(
        r"Author: (\S*)$",
        local("darcs show repo", capture=True),
        re.M
    ).group(1)
    return env.darcs_author
