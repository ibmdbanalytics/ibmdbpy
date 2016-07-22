.. highlight:: python

Geospatial Extension
********************

This module demonstrates the wrapper for spatial functions that ibmdbpy supports to generate and analyze spatial
information about geographic features, and to store and manage the data on which this information is based.
The spatial data is identified by ibmdbpy as a special class called IdaGeoDataFrame that extends all the properties
of an IdaDataFrame and has additional methods supported for geospatial types like ST_Point, ST_LineString, ST_Polygon
etc.

The simplest spatial data item consists of two coordinates that define the position of a single geographic feature
denoted with the type ST_Point. A more extensive spatial data item consists of several coordinates that define a linear
path such as a road or river might form, denoted as ST_LineString. A third kind consists of coordinates that define the
boundary of an area; for example, the boundary of a land parcel or flood plain, denoted as ST_Polygon.
Each spatial data item is an instance of a spatial data type. These types,together with the other spatial data types,
are structured types that belong to a single hierarchy ST_Geometry.

The python wrappers for spatial functions which DB2 currently supports make the querying process much simpler
for the users. These functions are broadly classified into two categories that have a single input and a double
input respectively. The single input functions work on a single IdaGeoDataFrame with one spatial column,
whereas the double input functions can either work on a single IdaGeoDataFrame with two spatial columns or two
different IdaGeoDataFrames with one spatial column each.

The project is still at an early stage and many of its features are still in development.
However, several experiments have already demonstrated that it provides significant performance advantages when
operating on medium or large amounts of data, that is, on tables of 1 million rows or more.

The latest version of ibmdbpy is available on the `Python Package Index`__.

__ https://pypi.python.org/pypi/ibmdbpy

How the spatial functions work
------------------------------

The ibmdbpy-spatial functions translate geopandas-like syntax into SQL and uses a middleware API (pypyodbc/JayDeBeApi)
to send it to an ODBC or JDBC-connected database for execution.
The results are fetched and formatted into the corresponding data structure, for example, a GeoPandas.GeoDataframe.

The following scenario illustrates how ibmdbpy works.

Assuming that all ODBC connection parameters are correctly set, issue the following statements to connect to a
database (in this case, a dashDB instance named DASHDB) via ODBC:

    >>> from ibmdbpy import IdaDataBase, IdaGeoDataFrame
    >>> idadb = IdaDataBase('DASHDB')

We can create an IDA geo data frame that points to a sample table in dashDB:

    >>> idadf = IdaGeoDataFrame(idadb, 'SAMPLES.GEO_COUNTY')

Note that to create an IDA geo data frame using the IdaDataFrame object, we need to specify our previously opened
IdaDataBase object, because it holds the connection.

Now let us compute the area of the counties in the GEO_COUNTY table:

    >>> idadf['area'] = idadf.area(colx = 'SHAPE')
         	OBJECTID 	NAME 	        SHAPE 	                                                 area
         	1 	        Wilbarger 	MULTIPOLYGON (((-99.4756582604 33.8340108094, ... 	0.247254
         	2 	        Austin 	        MULTIPOLYGON (((-96.6219873342 30.0442882117, ... 	0.162639
         	3 	        Logan 	        MULTIPOLYGON (((-99.4497297204 46.6316377481, ... 	0.306589
         	4 	        La Plata 	MULTIPOLYGON (((-107.4817473750 37.0000108736,... 	0.447591
         	5 	        Randolph 	MULTIPOLYGON (((-91.2589262966 36.2578866492, ... 	0.170844

The result of the area will be stored as a new column 'area' in the Ida geo data frame.



In the background, ibmdbpy-spatial looks for geometry columns in the table and builds an SQL request that returns
the area of each geometry.
Here is the SQL request that was executed for this example:

    SELECT *,ST_Area(SHAPE) AS "area" FROM SAMPLES.GEO_COUNTY;


It's as simple as that!

.. toctree::
   geoFrame.rst
   geoSeries.rst
