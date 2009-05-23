#!/usr/bin/env python

import os
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

def readfile(path):
    f = open(path, 'r')
    try:
        return f.read()
    finally:
        f.close()


setup(
    name='yoyo.migrate',
    description='Database migrations tool, using SQL and DB-API',
    long_description=readfile(
        os.path.join(os.path.dirname(__file__), 'README')
    ).decode('utf-8').encode('ascii', 'replace'),

    author='Oliver Cope',
    author_email='oliver@redgecko.org',
    scripts=['yoyo-migrate'],
    install_requires=[
    ],
    extras_require = {
        'mysql': [u'mysql-python'],
        'postgres': [u'psycopg2'],
    },
    dependency_links=[
        'http://sourceforge.net/project/showfiles.php?group_id=22307'
    ],
    namespace_packages=['yoyo'],
    packages=['yoyo.migrate'],
)

