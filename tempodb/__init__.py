
#!/usr/bin/env python
# encoding: utf-8
"""
tempodb/setup.py

Copyright (c) 2012 TempoDB Inc. All rights reserved.
"""

import client
from client import *


try:
    VERSION = __import__('pkg_resources') \
        .get_distribution('tempodb').version
except Exception, e:
    VERSION = 'unknown'


def get_version():
    return VERSION

__version__ = get_version()
