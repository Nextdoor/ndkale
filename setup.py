#!/usr/bin/env python
# -*- coding: utf-8 -*-

import multiprocessing  # To make python setup.py test happy
import os
import shutil
import subprocess

from distutils.command.clean import clean
from distutils.command.sdist import sdist
from setuptools import setup

multiprocessing

PACKAGE = 'nd-kale'
__version__ = None
execfile(os.path.join('kale', 'version.py'))  # set __version__


# -*- Hooks -*-

class SourceDistHook(sdist):

    def run(self):
        with open('version.rst', 'w') as f:
            f.write(':Version: %s\n' % __version__)
        shutil.copy('README.md', 'README.md')
        sdist.run(self)
        os.unlink('README.md')
        os.unlink('version.rst')


class CleanHook(clean):

    def run(self):
        clean.run(self)

        def maybe_rm(path):
            if os.path.exists(path):
                shutil.rmtree(path)

        if self.all:
            maybe_rm('nd_kale.egg-info')
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
    Programming Language :: Python :: 2.7
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
    setup_requires=[
        'flake8'
    ],
    test_suite='nose.collector',
    install_requires=[
        'boto',
        'python-gflags',
        'pycrypto',
        'pyyaml',
        'setuptools',
    ],
    classifiers=classifiers,
    cmdclass={'sdist': SourceDistHook, 'clean': CleanHook},
)
