.. Ibmdbpy documentation master file, created by
   sphinx-quickstart on Tue Jul 14 13:18:19 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

ibmdbpy-spatial
****************

Accelerating Geospatial Analytics by In-Database Processing
===========================================================

The ibmdbpy-spatial project provides a Python interface for data manipulation and access to in-database algorithms in IBM dashDB and IBM DB2.
It accelerates Python analytics by seamlessly pushing spatial operations written in Python into the underlying database for execution,
thereby benefitting from in-database performance-enhancing features, such as columnar storage and parallel processing.

IBM dashDB is a database management system available on IBM Bluemix, the cloud application development and analytics platform powered by IBM.
The ibmdbpy-spatial project can be used by Python developers with very little additional knowledge, because it copies the well-known interface of the geopandas library for data manipulation.

The ibmdbpy-spatial project is compatible with Python releases 2.7 up to 3.4 and can be connected to dashDB or DB2 instances via ODBC or JDBC.

The project is still at an early stage and many of its features are still in development.
However, several experiments have already demonstrated that it provides significant performance advantages when operating on medium or large amounts of data,
that is, on tables of 1 million rows or more.

The latest version of ibmdbpy is available on the `Python Package Index`__.

__ https://pypi.python.org/pypi/ibmdbpy

How ibmdbpy-spatial works
-------------------------

The ibmdbpy-spatial project translates geopandas-like syntax into SQL and uses a middleware API (pypyodbc/JayDeBeApi) to send it to an ODBC or JDBC-connected database for execution.
The results are fetched and formatted into the corresponding data structure, for example, a GeoPandas.GeoDataframe.

The following scenario illustrates how ibmdbpy works.

Assuming that all ODBC connection parameters are correctly set, issue the following statements to connect to a database (in this case, a dashDB instance named DASHDB) via ODBC:

>>> from ibmdbpy import IdaDataBase, IdaGeoDataFrame
>>> idadb = IdaDataBase('DASHDB')

We can create an IDA geo data frame that points to a sample table in dashDB:

>>> idadf = IdaGeoDataFrame(idadb, 'SAMPLES.GEO_COUNTY')

Note that to create an IDA geo data frame using the IdaDataFrame object, we need to specify our previously opened IdaDataBase object, because it holds the connection.

Now let us compute the area of the counties in the GEO_COUNTY table:

>>> idadf['area'] = idadf.area(colx = 'SHAPE')

The result of the area will be stored as a new column 'area' in the Ida geo data frame:



In the background, ibmdbpy-spatial looks for geometry columns in the table and builds an SQL request that returns the area of each geometry.
Here is the SQL request that was executed for this example::

   SELECT t.*,db2gse.ST_Area(t.SHAPE) as area
   FROM SAMPLES.GEO_COUNTY t;

The result fetched by ibmdbpy is a tuple containing all values of the matrix. This tuple is formatted back into a Pandas.DataFrame and then returned::

    	OBJECTID 	NAME 	     SHAPE 	                                            area
        	1 	  Wilbarger 	MULTIPOLYGON (((-99.4756582604 33.8340108094, ... 	0.247254
 	        2 	  Austin 	    MULTIPOLYGON (((-96.6219873342 30.0442882117, ... 	0.162639
 	        3 	  Logan 	    MULTIPOLYGON (((-99.4497297204 46.6316377481, ... 	0.306589
 	        4 	  La Plata 	    MULTIPOLYGON (((-107.4817473750 37.0000108736,... 	0.447591
 	        5 	  Randolph 	    MULTIPOLYGON (((-91.2589262966 36.2578866492, ... 	0.170844

That's as simple as that!

Table of Contents
=================

.. highlight:: python

.. toctree::
   :maxdepth: 2

   geospatial.rst

Project Roadmap
===============

* Full test coverage (a basic coverage is already provided)
* Add more functions and improve what already exists. 


Contributors
============

The ibmdbpy- spatial project was initiated in March 2016, and contributed by Rafael Rodriguez Morales and Avipsa Roy at IBM Deutschland Reasearch & Development.

Indexes and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


