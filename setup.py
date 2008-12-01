#!/usr/bin/env python

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


from distutils.core import setup

setup(
    name='torque-migrations',
    description='Storm backed database migration script',
    author='Oliver Cope',
    author_email='oliver@thelettero.co.uk',
    url='http://www.thelettero.co.uk/products/',
    scripts=['scripts/torque-migrations'],
    install_requires=[
        u'Storm',
        u'mysql-python',
    ],
    dependency_links=[
        'http://sourceforge.net/project/showfiles.php?group_id=22307'
    ],
)

