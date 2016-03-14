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
# Always prefer setuptools over distutils
from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
#from os import path
import os

#here = path.abspath(path.dirname(__file__))

# Get the long description from the relevant file
#with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
#    longdesc = f.read()

#with open('README.rst', 'r', 'utf-8') as f:
#    longdesc = f.read()
#longdesc.replace("\r\n", "\n")

### This is a copy of README.rst as a string
### Small fix because we could find how to handle special characters
longdesc = """
ibmdbpy
*******

Accelerating Python Analytics by In-Database Processing
=======================================================

The ibmdbpy project provides a Python interface for data manipulation and access to in-database algorithms in IBM dashDB and IBM DB2. It accelerates Python analytics by seamlessly pushing operations written in Python into the underlying database for execution, thereby benefitting from in-database performance-enhancing features, such as columnar storage and parallel processing.

IBM dashDB is a database management system available on IBM Bluemix, the cloud application development and analytics platform powered by IBM. The ibmdbpy project can be used by Python developers with very little additional knowledge, because it copies the well-known interface of the Pandas library for data manipulation and the Scikit-learn library for the use of machine learning algorithms.

The ibmdbpy project is compatible with Python releases 2.7 up to 3.4 and can be connected to dashDB or DB2 instances via ODBC or JDBC.

The project is still at an early stage and many of its features are still in development. However, several experiments have already demonstrated that it provides significant performance advantages when operating on medium or large amounts of data, that is, on tables of 1 million rows or more.

The latest version of ibmdbpy is available on the `Python Package Index`__.

__ https://pypi.python.org/pypi/ibmdbpy

How ibmdbpy works
-----------------

The ibmdbpy project translates Pandas-like syntax into SQL and uses a middleware API (pypyodbc/JayDeBeApi) to send it to an ODBC or JDBC-connected database for execution. The results are fetched and formatted into the corresponding data structure, for example, a Pandas.Dataframe or a Pandas.Series.

The following scenario illustrates how ibmdbpy works.

Assuming that all ODBC connection parameters are correctly set, issue the following statements to connect to a database (in this case, a dashDB instance named DASHDB) via ODBC:

>>> from ibmdbpy import IdaDataBase, IdaDataFrame
>>> idadb = IdaDataBase('DASHDB')

A few sample data sets are included in ibmdbpy for you to experiment. We can firstly load the well-known IRIS table into this dashDB instance.

>>> from ibmdbpy.sampledata import iris
>>> idadb.as_idadataframe(iris, "IRIS")
<ibmdbpy.frame.IdaDataFrame at 0x7ad77f0>

Next, we can create an IDA data frame that points to the table we just uploaded. Let’s use that one:

>>> idadf = IdaDataFrame(idadb, 'IRIS')

Note that to create an IDA data frame using the IdaDataFrame object, we need to specify our previously opened IdaDataBase object, because it holds the connection. 

Now let us compute the correlation matrix:

>>> idadf.corr()

In the background, ibmdbpy looks for numerical columns in the table and builds an SQL request that returns the correlation between each pair of columns. Here is the SQL request that was executed for this example::

   SELECT CORRELATION("sepal_length","sepal_width"), 
   CORRELATION("sepal_length","petal_length"), 
   CORRELATION("sepal_length","petal_width"), 
   CORRELATION("sepal_width","petal_length"), 
   CORRELATION("sepal_width","petal_width"), 
   CORRELATION("petal_length","petal_width") 
   FROM IRIS

The result fetched by ibmdbpy is a tuple containing all values of the matrix. This tuple is formatted back into a Pandas.DataFrame and then returned::

                 sepal_length  sepal_width  petal_length  petal_width
   sepal_length      1.000000    -0.117570      0.871754     0.817941
   sepal_width      -0.117570     1.000000     -0.428440    -0.366126
   petal_length      0.871754    -0.428440      1.000000     0.962865
   petal_width       0.817941    -0.366126      0.962865     1.000000

Et voilà !

Project Roadmap
===============

* Full test coverage (a basic coverage is already provided)
* Add more functions and improve what already exists. 
* Add wrappers for several ML-Algorithms (Linear regression, Sequential patterns...) 
* Feature selection extension
* Add Spark as computational engine 

Contributors
============

The ibmdbpy project was initiated in April 2015, and developed by Edouard Fouché, at IBM Deutschland Reasearch & Development. 
More contributors might participate in the future. 
"""

#def read(fname):
#    return open(os.path.join(os.path.dirname(__file__), fname)).read()

classifiers = [
	    # How mature is this project? Common values are
	    #   3 - Alpha
	    #   4 - Beta
	    #   5 - Production/Stable
	    'Development Status :: 4 - Beta',

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
	    #'Programming Language :: Python :: 3.6',
	    'Programming Language :: Python :: Implementation :: CPython',

	    'Topic :: Database',
	    'Topic :: Scientific/Engineering',
	    'Topic :: Software Development'
	  ]

setup(name='ibmdbpy',
      version='0.1.0b25',
      install_requires=['pandas','numpy','future','six','pypyodbc','lazy'],
      # optional are jaydebeapi, pytest, sphinx, bokeh
      extras_require={
      	'jdbc':['jaydebeapi'],
      	'test':['pytest'],
      	'doc':['sphinx'],
      	'bench':['bokeh']
      },
      description='A Pandas-like SQL-wrapper for in-database analytics with IBM dashDB/DB2.',
      long_description=longdesc,
      url='https://github.com/ibmdbanalytics/ibmdbpy',
      author='IBM Corp.',
      author_email='fouche@de.ibm.com',
      license='BSD',
      classifiers=classifiers,
      keywords='data analytics database development IBM dashDB DB2 pandas scikitlearn scalability machine-learning knowledge discovery',
      packages=find_packages(exclude=['docs', 'tests*']),
      package_data={
      	'ibmdbpy.sampledata': ['*.txt']}
     )

