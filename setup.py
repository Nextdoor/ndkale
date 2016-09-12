#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import

import multiprocessing  # To make python setup.py test happy
import os
import shutil
import subprocess

from distutils.command.clean import clean
from setuptools import setup

PACKAGE = 'ndkale'
__version__ = None
with open(os.path.join('kale', 'version.py')) as f:
    source = f.read()
code = compile(source, os.path.join('kale', 'version.py'), 'exec')
exec(code)  # set __version__


# -*- Hooks -*-

class CleanHook(clean):

    def run(self):
        clean.run(self)

        def maybe_rm(path):
            if os.path.exists(path):
                shutil.rmtree(path)

        if self.all:
            maybe_rm('ndkale.egg-info')
            maybe_rm('build')
            maybe_rm('dist')
            subprocess.call('rm -rf *.egg', shell=True)
            subprocess.call('find . -name "*.pyc" -exec rm -rf {} \;',
                            shell=True)

# -*- Classifiers -*-

classes = """
    Development Status :: 5 - Production/Stable
    License :: OSI Approved :: BSD License
    Topic :: System :: Distributed Computing
    Topic :: Software Development :: Object Brokering
    Programming Language :: Python
    Programming Language :: Python
    Programming Language :: Python :: 2.7
    Programming Language :: Python :: 3.5
    Programming Language :: Python :: Implementation :: CPython
    Operating System :: OS Independent
"""
classifiers = [s.strip() for s in classes.split('\n') if s]

# -*- %%% -*-

setup(
    name=PACKAGE,
    version=__version__,
    description='Kale: A Task Worker Library from Nextdoor',
    long_description=open('README.md').read(),
    author='Nextdoor',
    author_email='eng@nextdoor.com',
    url='https://github.com/Nextdoor/ndkale',
    download_url='http://pypi.python.org/pypi/ndkale#downloads',
    license='Apache License, Version 2',
    keywords='kale nextdoor taskworker sqs python',
    packages=['kale'],
    tests_require=[
        'mock',
        'nose'
    ],
    test_suite='nose.collector',
    install_requires=[
        'boto',
        'pycrypto',
        'pyyaml',
        'setuptools',
        'six',
        'tblib',
        'future',
    ],
    classifiers=classifiers,
    cmdclass={'clean': CleanHook},
)
