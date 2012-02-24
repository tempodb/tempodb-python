#!/usr/bin/env python
# encoding: utf-8
"""
tempodb/setup.py

Copyright (c) 2012 TempoDB Inc. All rights reserved.
"""

import os
from setuptools import setup

def get_version(version_tuple):
    version = '%s.%s' % (version_tuple[0], version_tuple[1])
    if version_tuple[2]:
        version = '%s.%s' % (version, version_tuple[2])
    return version

# Dirty hack to get version number from tempodb/__init__.py - we can't
# import it as it depends on dateutil, requests, and simplejson which aren't
# installed until this file is read
init = os.path.join(os.path.dirname(__file__), 'tempodb', '__init__.py')
version_line = filter(lambda l: l.startswith('VERSION'), open(init))[0]
VERSION = get_version(eval(version_line.split('=')[-1]))

setup(
    name="tempodb",
    version=VERSION,
    author="TempoDB Inc",
    author_email="dev@tempo-db.com",
    description="A client for the TempoDB API",
    packages=["tempodb"],
    long_description="A client for the TempoDB API.",
    install_requires=[
        'python-dateutil==1.5',
        'requests',
        'simplejson',
    ]
)
