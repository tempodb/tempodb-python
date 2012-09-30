#!/usr/bin/env python
# encoding: utf-8
"""
tempodb/setup.py

Copyright (c) 2012 TempoDB Inc. All rights reserved.
"""

from setuptools import setup


install_requires = [
    'python-dateutil==1.5',
    'requests',
    'simplejson',
]

tests_require = [
    'mock',
    'unittest2',
]

setup(
    name="tempodb",
    version="0.2.0",
    author="TempoDB Inc",
    author_email="dev@tempo-db.com",
    url="http://github.com/getsentry/tempodb-python/",
    description="A client for the TempoDB API",
    packages=["tempodb"],
    long_description="A client for the TempoDB API.",
    dependency_links=[
        'http://github.com/tempodb/requests/tarball/development#egg=requests-0.11.1ssl'
    ],
    setup_requires=['nose>=1.0'],
    install_requires=install_requires,
    tests_require=tests_require,
)
