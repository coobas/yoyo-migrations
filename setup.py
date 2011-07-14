#!/usr/bin/env python

import os
import ez_setup
ez_setup.use_setuptools()
from setuptools import setup, find_packages
import yoyo.migrate

def read(*path):
    """
    Read and return content from ``path``
    """
    f = open(
        os.path.join(
            os.path.dirname(__file__),
            *path
        ),
        'r'
    )
    try:
        return f.read().decode('UTF-8')
    finally:
        f.close()


setup(
    name='yoyo-migrations',
    version=read('VERSION.txt').strip().encode('ASCII'),
    description='Database schema migration tool, using SQL and DB-API',
    long_description=read('README.txt') + '\n\n' + read('CHANGELOG.txt'),
    author='Oliver Cope',
    author_email='oliver@redgecko.org',
    url='',
    license='BSD',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        # -*- Extra requirements: -*-
    ],
    extras_require = {
        'mysql': [u'mysql-python'],
        'postgres': [u'psycopg2'],
    },
    tests_require = [
        'sqlite3',
    ],
    dependency_links=[
    ],
    entry_points= {
        'console_scripts': [
            'yoyo-migrate=yoyo.scripts.migrate:main'
        ],
    }
)
