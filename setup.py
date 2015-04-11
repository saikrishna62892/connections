#!/usr/bin/env python
import os
from setuptools import setup
from connections import __version__ as src_version

PKG_VERSION = os.environ.get('CONNECTIONS_PKG_VERSION', src_version)

install_requires = []

setup(
    name='connections',
    version=PKG_VERSION,
    py_modules=['connections'],
    keywords = ('connection', 'pool', 'retry', 'states', 'waiting'),
    author='Eric Tsanyen',
    author_email='eric@sunshake.com',
    maintainer='operetta',
    description='Universal Connection Pool',
    long_description='''
connections is a Universal Connection Pool with retry and states keep
''',
    url='http://github.com/operetta/connections',
    license='Apache License, Version 2.0',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    install_requires=install_requires,
)

