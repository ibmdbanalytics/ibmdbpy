ibmdbpy
*******

**This package is deprecated, because it is no longer being maintained.**

Accelerating Python Analytics by In-Database Processing
=======================================================

The ibmdbpy project provides a Python interface for data manipulation and access to in-database algorithms in IBM Db2. It accelerates Python analytics by seamlessly pushing operations written in Python into the underlying database for execution, thereby benefitting from in-database performance-enhancing features, such as columnar storage and parallel processing.

IBM Db2 is a database management system. The ibmdbpy project can be used by Python developers with very little additional knowledge, because it copies the well-known interface of the Pandas library for data manipulation and the Scikit-learn library for the use of machine learning algorithms.

The ibmdbpy project is compatible with Python releases 2.7 up to 3.6 and can be connected to Db2 databases via ODBC or JDBC.

The project is still at an early stage and many of its features are still in development. However, several experiments have already demonstrated that it provides significant performance advantages when operating on medium or large amounts of data, that is, on tables of 1 million rows or more.

The latest version of ibmdbpy is available on the `Python Package Index`__ and Github_.

__ https://pypi.python.org/pypi/ibmdbpy

.. _Github: https://github.com/ibmdbanalytics/ibmdbpy

How ibmdbpy works
-----------------

The ibmdbpy project translates Pandas-like syntax into SQL and uses a middleware API (pypyodbc/JayDeBeApi) to send it to an ODBC or JDBC-connected database for execution. After fetching the results, printing them will look similar to printing a pandas.DataFrame or a pandas.Series. Ibmdbpy essential abstractions are IdaDataFrames, similar to Pandas.DataFrames, and IdaSeries, similar to Pandas.Series.Ibmdbpy also provides you with tools dedicated to geospatial data: you will use IdaGeoDataFrames which can be compared to the GeoDataFrames from GeoPandas, the geospatial extension of Pandas. 

The following scenario illustrates how ibmdbpy works.

Assuming that all ODBC connection parameters are correctly set, issue the following statements to connect to a database (in this case, a Db2 database named BLUDB) via ODBC:

>>> from ibmdbpy import IdaDataBase, IdaDataFrame
>>> idadb = IdaDataBase('BLUDB')

A few sample data sets are included in ibmdbpy for you to experiment. We can firstly load the well-known IRIS table into this Db2 database.

>>> from ibmdbpy.sampledata import iris
>>> idadb.as_idadataframe(iris, "IRIS")
<ibmdbpy.frame.IdaDataFrame at 0x7ad77f0>

Next, we can create an IDA data frame (IDA stands for in-database Analytics) that points to the table we just uploaded. Let’s use this one:

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

How the geospatial functions work
---------------------------------

The ibmdbpy package provides a Python interface to the in-database geospatial functions of IBM Db2 Spatial Extender (db2gse). It identifies the geometry column for spatial tables and lets you perform spatial queries based upon this column. The results are fetched and formatted into the corresponding data structure, for example, an IdaGeoDataframe.

The following scenario illustrates how spatial functions work by creating an IDA geospatial data frame that points to a sample table in Db2:

>>> from ibmdbpy import IdaDataBase, IdaGeoDataFrame
>>> idadb = IdaDataBase('BLUDB')
>>> idadf = IdaGeoDataFrame(idadb, 'SAMPLES.GEO_COUNTY')

Note that to create an IdaGeoDataframe using the IdaDataFrame object, we need to specify our previously opened IdaDataBase object, because it holds the connection.

Now let us compute the area of the counties in the GEO_COUNTY table. The result of the area will be stored as a new column 'area' in the IdaGeoDataFrame:

>>> idadf['area'] = idadf.area(colx = 'SHAPE')
    OBJECTID    NAME         SHAPE                                              area
    1           Wilbarger    MULTIPOLYGON (((-99.4756582604 33.8340108094, ...  0.247254
    2           Austin       MULTIPOLYGON (((-96.6219873342 30.0442882117, ...  0.162639
    3           Logan        MULTIPOLYGON (((-99.4497297204 46.6316377481, ...  0.306589
    4           La Plata     MULTIPOLYGON (((-107.4817473750 37.0000108736,...  0.447591
    5           Randolph     MULTIPOLYGON (((-91.2589262966 36.2578866492, ...  0.170844


In the background, ibmdbpy looks for the column defined as **geometry** in the table and builds an SQL request that returns the area of each ST_MULTIPOLYGON object (a multipolygon is a collection of polygons).
Here is the SQL request that was executed for this example::

   SELECT t.*,db2gse.ST_Area(t.SHAPE) as area
   FROM SAMPLES.GEO_COUNTY t;

Feature Selection
=================

Ibmdbpy provides a range of functions to support efficient in-database feature selection, e.g. to estimate the relevance of attributes with respect to a particular target. Functions and documentation can be found in the submodule ``ibmdbpy.feature_selection``. 

Contributors
============

The ibmdbpy project was initiated in April 2015 at IBM Deutschland Reasearch & Development, Böblingen. 
Here is the list of the persons who contributed to the project, in the chronological order of their contribution:

- Edouard Fouché (core)
- Michael Wurst (core)
- William Moore (documentation)
- Craig Blaha (documentation)
- Rafael Rodriguez Morales (geospatial extension, core)
- Avipsa Roy (geospatial extension)
- Nicole Schoen (core)
- Toni Bollinger (core)
- Eva Feillet (core, geospatial extension, documentation)
