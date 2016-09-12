.. highlight:: python

IdaGeoDataFrame
***************
An IdaGeoDataFrame is a reference to a spatial table in a remote instance of dashDB.

The most important property of an IdaGeoDataFrame is that it always has a reference to one IdaGeoSeries column
that holds a special status. This IdaGeoSeries is referred to as the IdaGeoDataFrame‘s “geometry”.
When a spatial method is applied to an IdaGeoDataFrame (or a spatial attribute like area is called),
this commands will always act on the “geometry” attribute.

The “geometry” attribute – no matter its name – can be accessed through the geometry attribute
of an IdaGeoDataFrame.

Open an IdaGeoDataFrame
=======================
.. currentmodule:: ibmdbpy.geoFrame

.. autoclass:: IdaGeoDataFrame

   .. automethod:: __init__

Set geometry
------------
.. automethod:: IdaGeoDataFrame.set_geometry

Create from an IdaDataFrame
---------------------------
.. automethod:: IdaGeoDataFrame.from_IdaDataFrame

Get the geometry attribute
--------------------------
.. automethod:: IdaGeoDataFrame.geometry

Geospatial Methods that return an IdaGeoDataFrame
=================================================
Some geospatial methods operate on two IdaGeoDataFrames to return a result as a boolean or a new geometry.
Such methods can be accessed with the IdaGeoDataFrame object to return a new IdaGeoDataFrame with three columns
respectively, indexer of the first IdaGeoDataFrame with which the method is called, the indexer of the second
IdaGeoDataFrame which is passed as an argument to the method and a third column which contains the result of the
geometric operation between the geometry columns of the first and second IdaGeoDataFrames.

Contains
--------
.. automethod:: IdaGeoDataFrame.contains

Crosses
-------
.. automethod:: IdaGeoDataFrame.crosses

Difference
----------
.. automethod:: IdaGeoDataFrame.difference

Disjoint
--------
.. automethod:: IdaGeoDataFrame.disjoint

Distance
--------
.. automethod:: IdaGeoDataFrame.distance

Equals
------
.. automethod:: IdaGeoDataFrame.equals

Intersection
------------
.. automethod:: IdaGeoDataFrame.intersection

Intersects
----------
.. automethod:: IdaGeoDataFrame.intersects

Mbr_Intersects
--------------
.. automethod:: IdaGeoDataFrame.mbr_intersects

Overlaps
--------
.. automethod:: IdaGeoDataFrame.overlaps

Touches
-------
.. automethod:: IdaGeoDataFrame.touches

Union
-----
.. automethod:: IdaGeoDataFrame.union

Within
------
.. automethod:: IdaGeoDataFrame.within



