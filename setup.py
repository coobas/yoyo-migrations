#!/usr/bin/env python
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


import os
import re
from setuptools import setup, find_packages

VERSIONFILE = "yoyo/__init__.py"
install_requires = ['python-slugify', 'iniherit']


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
    url='https://bitbucket.org/ollyc/yoyo',
    author='Oliver Cope',
    author_email='oliver@redgecko.org',
    license='Apache',
    keywords=['migrations', 'migrate', 'database', 'db', 'alembic', 'south',
              'mysql', 'postgresql', 'sqlite'],
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
    extras_require={
        'mysql': ['PyMySQL'],
        'postgres': ['psycopg2>=2.7'],
        'pyodbc': ['pyodbc']
    },
    tests_require=['mock'],
    entry_points={
        'console_scripts': [
            'yoyo=yoyo.scripts.main:main',
            'yoyo-migrate=yoyo.scripts.main:main',
        ],
    }
)
