#!/usr/bin/env python

import os
import re
from setuptools import setup

VERSIONFILE = "yoyo/__init__.py"


def get_version():
    with open(VERSIONFILE, 'rb') as f:
        return re.search("^__version__\s*=\s*['\"]([^'\"]*)['\"]",
                           f.read().decode('UTF-8'), re.M).group(1)


def read(*path):
    """
    Return content from ``path`` as a string
    """
    with open(os.path.join(os.path.dirname(__file__), *path), 'rb') as f:
        return f.read().decode('UTF-8')


setup(
    name='yoyo-migrations',
    version=get_version(),
    description='Database schema migration tool using SQL and DB-API',
    long_description=read('README.rst') + '\n\n' + read('CHANGELOG.rst'),
    author='Oliver Cope',
    author_email='oliver@redgecko.org',
    license='BSD',
    packages=['yoyo'],
    include_package_data=True,
    zip_safe=False,
    extras_require={
        'mysql': [u'mysql-python'],
        'postgres': [u'psycopg2'],
    },
    tests_require=['sqlite3'],
    entry_points={
        'console_scripts': [
            'yoyo-migrate=yoyo.scripts.migrate:main'
        ],
    }
)
