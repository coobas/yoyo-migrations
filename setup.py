#!/usr/bin/env python

import os
from setuptools import setup, find_packages

def readfile(path):
    f = open(path, 'r')
    try:
        return f.read()
    finally:
        f.close()


setup(
    name='yoyo-migrations',
    description='Database schema migration tool, using SQL and DB-API',
    long_description=readfile(
        os.path.join(os.path.dirname(__file__), 'README')
    ).decode('utf-8').encode('ascii', 'replace'),

    author='Oliver Cope',
    author_email='oliver@redgecko.org',
    scripts=['scripts/yoyo-migrate'],
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
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    include_package_data=True,
)
