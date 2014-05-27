#!/usr/bin/env python
# encoding: utf-8
"""
tempodb/setup.py

Copyright (c) 2012 TempoDB Inc. All rights reserved.
"""

from setuptools import setup


install_requires = [
    'python-dateutil',
    'requests>=1.0',
    'simplejson',
    'pytz',
    'sphinx'
]

tests_require = [
    'mock',
    'unittest2',
]

setup(
    name="tempodb",
    version="1.0.0",
    author="TempoDB Inc",
    author_email="dev@tempo-db.com",
    url="http://github.com/tempodb/tempodb-python/",
    description="A client for the TempoDB API",
    packages=["tempodb", "tempodb.temporal", "tempodb.protocol"],
    long_description="A client for the TempoDB API.",
    dependency_links=[
    ],
    setup_requires=['nose>=1.0'],
    install_requires=install_requires,
    tests_require=tests_require,
)
