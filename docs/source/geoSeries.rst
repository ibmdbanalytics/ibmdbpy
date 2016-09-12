.. highlight:: python

IdaGeoSeries
************
The spatial methods of an IdaGeoDataFrame can be used to operate on the geometry attribute and wil return a IdaGeoSeries
object in each case.
An IdaGeoSeries is essentially a reference to a spatial column of dashDB/DB2 where each entry in the column is a
set of shapes corresponding to one observation represented by DB2GSE.ST_GEOMETRY. An entry may consist of only one
shape (like a ST_POINT/ST_LINESTRING/ST_POLYGON) or multiple shapes that are meant to be thought of as one observation
(like the many polygons that make up the County of Santa Cruz in California or a state like Connecticut).

dashDB has three basic classes of geometric objects (which are actually DB2 spatial objects following OGC guidelines):

    + ST_Point / ST_MultiPoint
    + ST_Linestring / ST_MultiLineString
    + ST_Polygon / ST_MultiPolygon

Open an IdaGeoSeries
====================
.. currentmodule:: ibmdbpy.geoSeries

.. autoclass:: IdaGeoSeries

   .. automethod:: __init__

Geospatial Methods which return an IdaGeoSeries
===============================================
Once the geometry property of the IdaGeoDataFrame is set, the geospatial methods of IdaGeoSeries can be accessed
with the IdaGeoDataFrame object. Currently the following methods are supported.

Area
----
.. automethod:: IdaGeoSeries.area

Boundary
--------
.. automethod:: IdaGeoSeries.boundary

Buffer
------
.. automethod:: IdaGeoSeries.buffer

Centroid
--------
.. automethod:: IdaGeoSeries.centroid

Convex Hull
-----------
.. automethod:: IdaGeoSeries.convex_hull

coordDim
--------
.. automethod:: IdaGeoSeries.coord_dim

Dimension
---------
.. automethod:: IdaGeoSeries.dimension

Envelope
--------
.. automethod:: IdaGeoSeries.envelope

End Point
---------
..automethod:: IdaGeoSeries.end_point

Exterior Ring
-------------
..automethod:: IdaGeoSeries.exterior_ring

Generalize
----------
.. automethod:: IdaGeoSeries.generalize

Geometry Type
-------------
.. automethod:: IdaGeoSeries.geometry_type

is 3d
-----
.. automethod:: IdaGeoSeries.is_3d

is Closed
---------
.. automethod:: IdaGeoSeries.is_closed

is Empty
--------
.. automethod:: IdaGeoSeries.is_empty

is Measured
-----------
.. automethod:: IdaGeoSeries.is_measured

is Simple
---------
.. automethod:: IdaGeoSeries.is_simple

is Valid
--------
.. automethod:: IdaGeoSeries.is_valid

Length
------
.. automethod:: IdaGeoSeries.length

max M
-----
.. automethod:: IdaGeoSeries.max_m

max X
-----
.. automethod:: IdaGeoSeries.max_x

max Y
-----
.. automethod:: IdaGeoSeries.max_y

max Z
-----
.. automethod:: IdaGeoSeries.max_z

MBR
---
.. automethod:: IdaGeoSeries.mbr

Mid Point
---------
.. automethod:: IdaGeoSeries.mid_point

min M
-----
.. automethod:: IdaGeoSeries.min_m

min X
-----
.. automethod:: IdaGeoSeries.min_x

min Y
-----
.. automethod:: IdaGeoSeries.min_y

min Z
-----
.. automethod:: IdaGeoSeries.min_z

num Geometries
--------------
.. automethod:: IdaGeoSeries.num_geometries

num Interior Ring
-----------------
.. automethod:: IdaGeoSeries.num_interior_ring

num Linestrings
---------------
.. automethod:: IdaGeoSeries.num_line_strings

num Points
----------
.. automethod:: IdaGeoSeries.num_points

num Polygons
------------
.. automethod:: IdaGeoSeries.num_polygons

perimeter
---------
.. automethod:: IdaGeoSeries.perimeter

Start Point
-----------
..automethod:: IdageoSeries.start_point

SR ID
-----
.. automethod:: IdaGeoSeries.srid

SRS Name
--------
.. automethod:: IdaGeoSeries.srs_name

X coordinate
------------
.. automethod:: IdaGeoSeries.x

Y coordinate
------------
.. automethod:: IdaGeoSeries.y

Z coordinate
------------
.. automethod:: IdaGeoSeries.z
