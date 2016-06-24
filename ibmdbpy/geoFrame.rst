.. highlight:: python

IdaGeoDataFrame
************

Open an IdaGeoDataFrame Object.
===============================

.. currentmodule:: ibmdbpy.geoFrame

.. autoclass:: IdaGeoDataFrame

   .. automethod:: __init__

.. rubric:: Methods


DataFrame introspection
=======================
.. autoclass:: ibmdbpy.geoFrame
	:members:

internal_state
--------------
.. autoattribute:: IdaDataFrame.internal_state

indexer
-------
.. autoattribute:: IdaDataFrame.indexer

type
----
.. autoattribute:: IdaDataFrame.type

dtypes
------
.. autoattribute:: IdaDataFrame.dtypes

index
-----
.. autoattribute:: IdaDataFrame.index

columns
-------
.. autoattribute:: IdaDataFrame.columns

axes
----
.. autoattribute:: IdaDataFrame.axes

shape
-----
.. autoattribute:: IdaDataFrame.shape

empty
-----
.. autoattribute:: IdaDataFrame.empty

__len__
-------
.. automethod:: IdaDataFrame.__len__

__iter__
---------
.. automethod:: IdaDataFrame.__iter__

DataFrame modification
======================

Selection, Projection
---------------------
.. automethod:: IdaGeoDataFrame.__getitem__
.. automethod:: IdaGeoDataFrame.__setitem__

Selection and Projection are also possible using the ``ibmdbpy.Loc`` object stored in ``IdaGeoDataFrame.loc``.

.. autoclass:: ibmdbpy.indexing.Loc
	:members:




Data Exploration
================
.. autoclass:: ibmdbpy.frame
head
----
.. automethod:: IdaDataFrame.head

tail
----
.. automethod:: IdaDataFrame.tail

Private Methods
===============
.. autoclass:: ibmdbpy.geoFrame
	:members:

_clone
------
.. automethod:: IdaGeoDataFrame._clone

_get_columns
------------
.. automethod:: IdaGeoDataFrame._get_columns

_reset_attributes
-----------------
.. automethod:: IdaGeoDataFrame._reset_attributes

SingleInput Methods
===================
.. autoclass:: ibmdbpy.geoFrame
	:members:

.. automethod:: IdaGeoDataFrame.buffer
.. automethod:: IdaGeoDataFrame.centroid
.. automethod:: IdaGeoDataFrame.boundary
.. automethod:: IdaGeoDataFrame.envelope
.. automethod:: IdaGeoDataFrame.MBR
.. automethod:: IdaGeoDataFrame.SRID
.. automethod:: IdaGeoDataFrame.srsName
.. automethod:: IdaGeoDataFrame.geometryType
.. automethod:: IdaGeoDataFrame.perimeter
.. automethod:: IdaGeoDataFrame.X
.. automethod:: IdaGeoDataFrame.Y
.. automethod:: IdaGeoDataFrame.Z
.. automethod:: IdaGeoDataFrame.area
.. automethod:: IdaGeoDataFrame.dimension
.. automethod:: IdaGeoDataFrame.length

