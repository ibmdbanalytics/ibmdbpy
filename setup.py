#!/usr/bin/env python
# -*- coding: utf-8 -*-
#-----------------------------------------------------------------------------
# Copyright (c) 2015, IBM Corp.
# All rights reserved.
#
# Distributed under the terms of the BSD Simplified License.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

"""
setup.py
"""
# Note: Always prefer setuptools over distutils
from setuptools import setup, find_packages
from codecs import open

# Get the long description from the relevant file
with open('README.rst', 'r', encoding='utf-8') as f:
    longdesc = f.read()

classifiers = [
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 7 - Inactive',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Intended Audience :: Education',
        'Intended Audience :: End Users/Desktop',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Science/Research',
        'Topic :: Software Development :: Build Tools',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: BSD License',

        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX',

        'Natural Language :: English',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',

        'Topic :: Database',
        'Topic :: Scientific/Engineering',
        'Topic :: Software Development'
      ]

setup(name='ibmdbpy',
      version='0.1.6.post1',
      install_requires=['pandas','numpy','future','six','pypyodbc','lazy'],
      # optional are jaydebeapi, pytest, sphinx, bokeh
      # execute "pip install -e .[jdbc] ibmdbpy" for installing ibmdbpy with the extra jdbc packages
      extras_require={
        'jdbc':['JayDeBeApi==1.*', 'Jpype1==0.6.3'],
        'test':['pytest', 'flaky==3.4.0'],
        'doc':['sphinx', 'ipython', 'numpydoc', 'sphinx_rtd_theme']
      },
      description='A Pandas-like SQL-wrapper for in-database analytics with IBM Db2 Warehouse.',
      long_description=longdesc,
      url='https://github.com/ibmdbanalytics/ibmdbpy',
      author='IBM Corp.',
      author_email='Toni.Bollinger@de.ibm.com',
      license='BSD',
      classifiers=classifiers,
      keywords='data analytics database development IBM Db2 Warehouse pandas scikitlearn scalability machine-learning knowledge discovery',
      packages=find_packages(exclude=['docs', 'tests*']),
      package_data={
        'ibmdbpy.sampledata': ['*.txt']}
     )
