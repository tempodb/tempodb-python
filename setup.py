#!/usr/bin/env python
# encoding: utf-8
"""
tempodb/setup.py

Copyright (c) 2012 TempoDB Inc. All rights reserved.
"""

import os
from setuptools import setup

import tempodb


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name="tempodb",
    version=tempodb.__version__,
    author="TempoDB Inc",
    author_email="dev@tempo-db.com",
    description="A client for the TempoDB API",
    packages=["tempodb"],
    long_description=read('README'),
    install_requires=[
        'python-dateutil==1.5',
        'requests',
        'simplejson',
    ]
)
