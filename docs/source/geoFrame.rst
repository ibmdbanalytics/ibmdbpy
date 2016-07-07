.. highlight:: python

Geospatial Functions
********************
The spatial functions of ibmdbpy can operate in two different ways. It can either operate on a single spatial column of
one table and calculate features like area, length, dimension , perimeter etc for a single geometry. In another case, it
can work on two different spatial columns in a single IdaGeoDataFrame or one spatial column each from two different
IdaGeoDataFrames. Here we elaborate on the structure of the wrapper methods in ibmdbpy for these functions.

.. currentmodule:: ibmdbpy.geoFrame

.. autoclass:: ibmdbpy.geoFrame
	:members:

.. automethod:: IdaGeoDataFrame.area
.. automethod:: IdaGeoDataFrame.boundary
.. automethod:: IdaGeoDataFrame.buffer
.. automethod:: IdaGeoDataFrame.centroid
.. automethod:: IdaGeoDataFrame.coordDim
.. automethod:: IdaGeoDataFrame.dimension
.. automethod:: IdaGeoDataFrame.envelope
.. automethod:: IdaGeoDataFrame.geometryType
.. automethod:: IdaGeoDataFrame.is3d
.. automethod:: IdaGeoDataFrame.isClosed
.. automethod:: IdaGeoDataFrame.isEmpty
.. automethod:: IdaGeoDataFrame.isMeasured
.. automethod:: IdaGeoDataFrame.isSimple
.. automethod:: IdaGeoDataFrame.isValid
.. automethod:: IdaGeoDataFrame.length
.. automethod:: IdaGeoDataFrame.maxM
.. automethod:: IdaGeoDataFrame.maxX
.. automethod:: IdaGeoDataFrame.maxY
.. automethod:: IdaGeoDataFrame.maxZ
.. automethod:: IdaGeoDataFrame.MBR
.. automethod:: IdaGeoDataFrame.minM
.. automethod:: IdaGeoDataFrame.minX
.. automethod:: IdaGeoDataFrame.minY
.. automethod:: IdaGeoDataFrame.minZ
.. automethod:: IdaGeoDataFrame.numGeometries
.. automethod:: IdaGeoDataFrame.numInteriorRing
.. automethod:: IdaGeoDataFrame.numLineStrings
.. automethod:: IdaGeoDataFrame.numPoints
.. automethod:: IdaGeoDataFrame.numPolygons
.. automethod:: IdaGeoDataFrame.perimeter
.. automethod:: IdaGeoDataFrame.SRID
.. automethod:: IdaGeoDataFrame.srsName
.. automethod:: IdaGeoDataFrame.X
.. automethod:: IdaGeoDataFrame.Y
.. automethod:: IdaGeoDataFrame.Z











