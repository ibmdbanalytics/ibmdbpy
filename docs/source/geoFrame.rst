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
