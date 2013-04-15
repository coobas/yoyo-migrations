#!/usr/bin/env python

import os
from setuptools import setup


def read(*path):
    """
    Read and return content from ``path``
    """
    with open(os.path.join(os.path.dirname(__file__), *path), 'rb') as f:
        return f.read().decode('UTF-8')


setup(
    name='yoyo-migrations',
    version=read('VERSION.txt').strip().encode('ASCII'),
    description='Database schema migration tool, using SQL and DB-API',
    long_description=read('README.rst') + '\n\n' + read('CHANGELOG.rst'),
    author='Oliver Cope',
    author_email='oliver@redgecko.org',
    url='',
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
