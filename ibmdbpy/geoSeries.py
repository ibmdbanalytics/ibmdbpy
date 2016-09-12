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
IdaGeoSeries
"""

# Ensure Python 2 compatibility
from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals
from __future__ import absolute_import
from builtins import map
from builtins import super
from future import standard_library
standard_library.install_aliases()

from numbers import Number
from collections import OrderedDict

from lazy import lazy
import six

import ibmdbpy
from ibmdbpy.series import IdaSeries
from ibmdbpy.exceptions import IdaGeoDataFrameError

class IdaGeoSeries(ibmdbpy.IdaSeries):
    """
    An IdaSeries whose column must have geometry type.
    It has geospatial methods based on DashDB/DB2 Spatial Extender (DB2GSE).
    
    # TODO: add examples

    Notes:
    ------
    An IdaGeoSeries doesn't have an indexer attribute because geometries are
    unorderable in DB2 Spatial Extender.
    """
    def __init__(self, idadb, tablename, indexer, column):
        """
        Ensures that the specified column has geometry type.
        See __init__ of IdaSeries.

        Parameters
        ----------
        column : str
            Column name. It must have geometry type.

        Notes
        -----
        Even though geometry types are unorderable in DB2GSE, the IdaGeoSeries
        might have as indexer another column of the table whose column the
        IdaGeoSeries refers to.
        """
        super(IdaGeoSeries, self).__init__(idadb, tablename, indexer, column)
        if self.dtypes.TYPENAME[self.column].find('ST_') != 0:
            raise TypeError("Specified column doesn't have geometry type. "
                            "Cannot create IdaGeoSeries object")

    @classmethod
    def from_IdaSeries(cls, idaseries):
        """
        Creates an IdaGeoSeries from an IdaSeries, ensuring that the column
        of the given IdaSeries has geometry type.
        """
        if not isinstance(idaseries, IdaSeries):
            raise TypeError("Expected IdaSeries")
        else:
            # Mind that the given IdaSeries might have non-destructive
            # columns that were added by the user. That's why __init__ is not
            # used for this purpose.
            if idaseries.dtypes.TYPENAME[idaseries.column].find('ST_') != 0:
                raise TypeError(
                    "The column of the IdaSeries doesn't have geometry type. "
                    "Cannot create IdaGeoSeries object")
            else:
                idageoseries = idaseries
                idageoseries.__class__ = IdaGeoSeries
                return idageoseries

#==============================================================================
### Methods whose behavior is not defined for geometry types in DB2GSE.
#==============================================================================

    # TODO: Override all the methods of IdaSeries (and those of its parent,
    # i.e. IdaDataFrame, which are not defined in DB2GSE for geometry columnns,
    # like min(), max(), etc.)

    def min(self):
        raise TypeError("Unorderable geometries")
        pass

    def max(self):
        raise TypeError("Unorderable geometries")
        pass

#==============================================================================
### Unary geospatial methods
#==============================================================================

    def generalize(self, threshold):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaGeoSeries of geometries which represent each of the
        geometries in the calling IdaGeoSeries, but with a reduced number of
        points, while preserving the general characteristics of the geometry.

        The Douglas-Peucker line-simplification algorithm is used, by which the
        sequence of points that define the geometry is recursively subdivided
        until a run of the points can be replaced by a straight line segment.
        In this line segment, none of the defining points deviates from the
        straight line segment by more than the given threshold. Z and M
        coordinates are not considered for the simplification. The resulting
        geometry is in the spatial reference system of the given geometry.

        For empty geometries, the output is an empty geometry of type ST_Point.
        For None geometries the output is None.

        Parameters
        ----------
        threshold : float
            Threshold to be used for the line-simplification algorithm.
            The threshold must be greater than or equal to 0.
            The larger the threshold, the smaller the number of points that
            will be used to represent the generalized geometry.

        Returns
        -------
        IdaGeoSeries.

        References
        ----------
        DB2 Spatial Extender ST_GENERALIZE() function.

        Examples
        --------
        >>> tornadoes = IdaGeoDataFrame(idadb,'SAMPLES.GEO_TORNADO',indexer='OBJECTID')
        >>> tornadoes.set_geometry('SHAPE')
        >>> tornadoes['generalize'] = tornadoes.generalize(threshold = 4)
        >>> tornadoes[['OBJECTID','generalize']].head()
        OBJECTID  generalize
        1         MULTILINESTRING ((-90.2200062071 38.7700071663...
        2         MULTILINESTRING ((-89.3000059755 39.1000072739...
        3         MULTILINESTRING ((-84.5800047496 40.8800078382...
        4         MULTILINESTRING ((-94.3700070010 34.4000061520...
        5         MULTILINESTRING ((-90.6800062393 37.6000069289...
        """
        try:
            threshold = float(threshold)
        except:
            raise TypeError("threshold must be float")        
        if threshold < 0:
            raise ValueError("threshold must be greater than or equal to 0")
        additional_args = [threshold]
        return self._unary_operation_handler(
            db2gse_function = 'DB2GSE.ST_GENERALIZE',
            valid_types = ['ST_GEOMETRY'],
            additional_args = additional_args)

    def buffer(self, distance, unit = None):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaGeoSeries of geometries in which each point is the
        specified distance away from the geometries in the calling
        IdaGeoSeries, measured in the given unit.

        Parameters
        ----------
        distance : float
            Distance, can be positive or negative.
        unit : str, optional
            Name of the unit, it is case-insensitive.
            If omitted, the following rules are used:
                * If geometry is in a projected or geocentric coordinate
                system, the linear unit associated with this coordinate system
                is the default.
                * If geometry is in a geographic coordinate system, the angular
                unit associated with this coordinate system is the default.

        Returns
        -------
        IdaGeoSeries.

        See also
        --------
        linear_units : list of valid units.

        Notes
        -----
        Restrictions on unit conversions: An error (SQLSTATE 38SU4) is returned
        if any of the following conditions occur:
            * The geometry is in an unspecified coordinate system and the unit
            parameter is specified.
            * The geometry is in a projected coordinate system and an angular
            unit is specified.
            * The geometry is in a geographic coordinate system, but is not an
            ST_Point value , and a linear unit is specified.
        # TODO: handle this SQLSTATE error

        References
        ----------
        DB2 Spatial Extender ST_BUFFER() function.

        Examples
        --------
        >>> tornadoes = IdaGeoDataFrame(idadb,'SAMPLES.GEO_TORNADO',indexer='OBJECTID')
        >>> tornadoes.set_geometry('SHAPE')
        >>> tornadoes['buffer_20_km'] = tornadoes.buffer(distance = 20, unit = 'KILOMETER')
        >>> tornadoes[['OBJECTID','SHAPE','buffer_20_km']].head()
        OBJECTID  SHAPE                                        buffer_20_km
        1         MULTILINESTRING ((-90.2200062071 38.770....  POLYGON ((-90.3065519651 38.9369737029, -90.32..
        2         MULTILINESTRING ((-89.3000059755 39.100....  POLYGON ((-89.3798853739 39.2690904737, -89.39.
        3         MULTILINESTRING ((-84.5800047496 40.880....  POLYGON ((-84.7257488606 41.0222185578, -84.73...
        4         MULTILINESTRING ((-94.3700070010 34.400....  POLYGON ((-94.5212609425 34.5296645617, -94.53...
        5         MULTILINESTRING ((-90.6800062393 37.600....  POLYGON ((-90.8575378881 37.7120296620, -90.86...
        """
        if not isinstance(distance, Number):
            # distance can be positive or negative
            raise TypeError("Distance must be numerical")
        additional_args = []
        additional_args.append(distance)
        if unit is not None:
            unit = self._check_linear_unit(unit)  # Can raise exceptions
            additional_args.append(unit)
        return self._unary_operation_handler(
            db2gse_function = 'DB2GSE.ST_BUFFER',
            valid_types = ['ST_GEOMETRY'],
            additional_args = additional_args)

    def centroid(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaGeoSeries of points which represent the geometric center
        of each of the geometries in the calling IdaGeoSeries.

        The geometric center is the center of the minimum bounding rectangle of
        the given geometry, as a point.

        The resulting point is represented in the spatial reference system of
        the given geometry.

        For None geometries the output is None.

        Returns
        -------
        IdaGeoSeries.

        References
        ----------
        DB2 Spatial Extender ST_CENTROID() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>> counties['centroid'] = counties.centroid()
        >>> counties[['NAME','centroid']].head()
        NAME         centroid
        Wood         POINT (-83.6490410160 41.3923524865)
        Cass         POINT (-94.3483719161 33.0944709011)
        Washington   POINT (-89.4241634562 38.3657576429)
        Fulton       POINT (-74.4337987380 43.1359187016)
        Clay         POINT (-96.5066339619 46.8908550036)
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_CENTROID',
                valid_types = ['ST_GEOMETRY'])

    def convex_hull(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaGeoSeries of geometries which are the convex hull of each
        of the geometries in the calling IdaGeoSeries.

        The resulting geometry is represented in the spatial reference system
        of the given geometry.

        If possible, the specific type of the returned geometry will be
        ST_Point, ST_LineString, or ST_Polygon. For example, the boundary of a
        polygon with no holes is a single linestring, represented as
        ST_LineString. The boundary of a polygon with one or more holes
        consists of multiple linestrings, represented as ST_MultiLineString.

        For None geometries the output is None.
        For empty geometries the output is None.

        Returns
        -------
        IdaGeoSeries.

        References
        ----------
        DB2 Spatial Extender ST_CONVEXHULL() function.

        Examples
        --------
        # TODO add examples
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_CONVEXHULL',
                valid_types = ['ST_GEOMETRY'])

    def boundary(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaGeoSeries of geometries which are the boundary of each
        of the geometries in the calling IdaGeoSeries.

        The resulting geometry is represented in the spatial reference system
        of the given geometry.

        If the given geometry is a point, multipoint, closed curve, or closed
        multicurve, or if it is empty, then the result is an empty geometry of
        type ST_Point. For curves or multicurves that are not closed, the start
        points and end points of the curves are returned as an ST_MultiPoint
        value, unless such a point is the start or end point of an even number
        of curves. For surfaces and multisurfaces, the curve defining the
        boundary of the given geometry is returned, either as an ST_Curve or an
        ST_MultiCurve value.

        If possible, the specific type of the returned geometry will be
        ST_Point, ST_LineString, or ST_Polygon. For example, the boundary of a
        polygon with no holes is a single linestring, represented as
        ST_LineString. The boundary of a polygon with one or more holes
        consists of multiple linestrings, represented as ST_MultiLineString.

        For None geometries the output is None.

        Returns
        -------
        IdaGeoSeries.

        References
        ----------
        DB2 Spatial Extender ST_BOUNDARY() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>> counties['boundary'] = counties.boundary()
        >>> counties[['NAME','boundary']].head()
        NAME         boundary
        Madison      LINESTRING (-90.4500428418 32.5737889565, -90....
        Lake         LINESTRING (-114.6043395348 47.7897504535, -11...
        Broward      LINESTRING (-80.8798118938 26.2594597939, -80....
        Buena Vista  LINESTRING (-95.3880180283 42.5617494883, -95....
        Jones        LINESTRING (-77.0903250894 34.8027619185, -77..
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_BOUNDARY',
                valid_types = ['ST_GEOMETRY'])

    def envelope(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry.

        Returns an IdaGeoSeries of polygons which are an envelope around each
        of the geometries in the calling IdaGeoSeries. The envelope is a
        rectangle that is represented as a polygon.

        If the given geometry is a point, a horizontal linestring, or a
        vertical linestring, then a rectangle, which is slightly larger than
        the given geometry, is returned. Otherwise, the minimum bounding
        rectangle of the geometry is returned as the envelope.

        For None geometries the output is None.
        For empty geometries the output is None.

        Returns
        -------
        IdaGeoSeries.

        See also
        --------
        mbr

        References
        ----------
        DB2 Spatial Extender ST_ENVELOPE() function.

        Examples
        --------
        >>> tornadoes = IdaGeoDataFrame(idadb,'SAMPLES.GEO_TORNADO',indexer='OBJECTID')
        >>> tornadoes.set_geometry('SHAPE')
        >>> tornadoes['envelope'] = tornadoes.envelope()
        >>> tornadoes[['OBJECTID', 'SHAPE', 'envelope']].head()
        OBJECTID   SHAPE                                      envelope
        1          MULTILINESTRING ((-90.2200062071 38.77..   POLYGON ((-90.2200062071 38.77..
        2          MULTILINESTRING ((-89.3000059755 39.10..   POLYGON ((-89.3000059755 39.10..
        3          MULTILINESTRING ((-84.5800047496 40.88..   POLYGON ((-84.5800047496 40.88..
        4          MULTILINESTRING ((-94.3700070010 34.40..   POLYGON ((-94.3700070010 34.40..
        5          MULTILINESTRING ((-90.6800062393 37.60..   POLYGON ((-90.6800062393 37.60..
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_ENVELOPE',
                valid_types = ['ST_GEOMETRY'])

    def exterior_ring(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Polygon.

        Returns an IdaGeoSeries of curves which are the exterior ring of each
        of the geometries in the calling IdaGeoSeries.

        The resulting curve is represented in the spatial reference system of
        the given polygon.

        If the polygon does not have any interior rings, the returned exterior
        ring is identical to the boundary of the polygon.

        For None polygons the output is None.
        For empty polygons the output is None.

        Returns
        -------
        IdaGeoSeries.

        References
        ----------
        DB2 Spatial Extender ST_EXTERIORRING() function.

        Examples
        --------
        # TODO add examples

        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_EXTERIORRING',
                valid_types = ['ST_POLYGON'])

    def mbr(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaGeoSeries of geometries which are the minimum bounding
        rectangle of each of the geometries in the calling IdaGeoSeries.

        If the given geometry is a point, then the point itself is returned.
        If the geometry is a horizontal linestring or a vertical linestring,
        the horizontal or vertical linestring itself is returned.

        For None geometries the output is None.
        For empty geometries the output is None.

        Returns
        -------
        IdaGeoSeries.

        References
        ----------
        DB2 Spatial Extender ST_MBR() function.

        Examples
        --------
        # TODO add examples
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_MBR',
                valid_types = ['ST_GEOMETRY'])

    def end_point(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_LINESTRING.

        Returns an IdaGeoSeries with the last point of each of the curves in
        the calling IdaGeoSeries.

        The resulting point is represented in the spatial reference system of
        the given curve.

        For None curves the output is None.
        For empty curves the output is None.

        Returns
        -------
        IdaGeoSeries.

        References
        ----------
        DB2 Spatial Extender ST_ENDPOINT() function.

        Examples
        --------
        # TODO add examples
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_ENDPOINT',
                valid_types = ['ST_LINESTRING'])

    def mid_point(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_LINESTRING.

        Returns an IdaGeoSeries of points which are equidistant from both ends
        of each of the curves in the calling IdaGeoSeries, measured along the
        curve.

        The resulting point is represented in the spatial reference system of
        the given curve.

        If the curve contains Z coordinates or M coordinates (measures),
        the midpoint is determined solely by the values of the X and Y
        coordinates in the curve. The Z coordinate and measure in the returned
        point are interpolated.

        For None curves the output is None.
        For empty curves, the output is an empty point.

        Returns
        -------
        IdaGeoSeries.

        References
        ----------
        DB2 Spatial Extender ST_MIDPOINT() function.

        Examples
        --------
        # TODO add examples
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_MIDPOINT',
                valid_types = ['ST_LINESTRING'])

    def start_point(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_LINESTRING.

        Returns an IdaGeoSeries with the first point of each of the curves in
        the calling IdaGeoSeries.

        The resulting point is represented in the spatial reference system of
        the given curve.

        For None curves the output is None.
        For empty curves the output is None.

        Returns
        -------
        IdaGeoSeries.

        References
        ----------
        DB2 Spatial Extender ST_STARTPOINT() function.

        Examples
        --------
        # TODO add examples
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_STARTPOINT',
                valid_types = ['ST_LINESTRING'])

    def srid(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with integers representing the spatial reference
        system of each of the geometries in the calling IdaGeoSeries.

        For None geometries the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        DB2 Spatial Extender ST_SRID() function.

        Examples
        --------
        # TODO add examples
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_SRID',
                valid_types = ['ST_GEOMETRY'])

    def srs_name(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with strings representing the name of the spatial
        reference system of each of the geometries in the calling IdaGeoSeries.

        Returns
        -------
        IdaSeries.

        References
        ----------
        DB2 Spatial Extender ST_SRSNAME() function.

        Examples
        --------
        >>> tornadoes = IdaGeoDataFrame(idadb,'SAMPLES.GEO_TORNADO',indexer='OBJECTID')
        >>> tornadoes.set_geometry('SHAPE')
        >>> tornadoes['srs_name'] = tornadoes.srs_name()
        >>> tornadoes[['OBJECTID', 'SHAPE', 'srs_name']].head()
        OBJECTID   SHAPE 	                                            srs_name
        1 	       MULTILINESTRING ((-90.2200062071 38.7700071663...    SAMPLE_GCS_WGS_1984
        2 	       MULTILINESTRING ((-89.3000059755 39.1000072739...    SAMPLE_GCS_WGS_1984
        3 	       MULTILINESTRING ((-84.5800047496 40.8800078382...    SAMPLE_GCS_WGS_1984
        4 	       MULTILINESTRING ((-94.3700070010 34.4000061520...    SAMPLE_GCS_WGS_1984
        5 	       MULTILINESTRING ((-90.6800062393 37.6000069289...    SAMPLE_GCS_WGS_1984
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_SRSNAME',
                valid_types = ['ST_GEOMETRY'])

    def geometry_type(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry.

        Returns an IdaSeries with strings representing the fully qualified type
        name of the dynamic type of each of the geometries in the calling
        IdaGeoSeries.

        Returns
        -------
        IdaSeries.

        References
        ----------
        DB2 Spatial Extender ST_GEOMETRYTYPE() function.

        Examples
        --------
        # TODO add examples
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_GEOMETRYTYPE',
                valid_types = ['ST_GEOMETRY'])

    def area(self, unit = None):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with doubles representing the area covered by
        each of the geometries in the calling IdaGeoSeries, in the given unit
        or else in the default unit.

        If the geometry is a polygon or multipolygon, then the area covered by
        the geometry is returned. The area of points, linestrings, multipoints,
        and multilinestrings is 0 (zero).

        For None geometries the output is None.
        For empty geometries the output is None.

        Parameters
        ----------
        unit : str, optional
            Name of the unit, it is case-insensitive.
            If omitted, the following rules are used:
                * If geometry is in a projected or geocentric coordinate
                system, the linear unit associated with this coordinate system
                is used.
                * If geometry is in a geographic coordinate system, the angular
                unit associated with this coordinate system is used.

        Returns
        -------
        IdaSeries.

        See also
        --------
        linear_units : list of valid units.

        Notes
        -----
        Restrictions on unit conversions: An error (SQLSTATE 38SU4) is returned
        if any of the following conditions occur:
            * The geometry is in an unspecified coordinate system and the unit
            parameter is specified.
            * The geometry is in a projected coordinate system and an angular
            unit is specified.
            * The geometry is in a geographic coordinate system, and a linear
            unit is specified.
        # TODO: handle this SQLSTATE error

        References
        ----------
        DB2 Spatial Extender ST_AREA() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>> counties['area_in_km'] = counties.area(unit = 'KILOMETER')
        >>> counties[['NAME','area_in_km']].head()
        NAME         area_in_km
        Wood         1606.526429
        Cass         2485.836511
        Washington   1459.393496
        Fulton       1382.620091
        Clay         2725.095566
        """
        additional_args = []
        if unit is not None:
            unit = self._check_linear_unit(unit)  # Can raise exceptions
            additional_args.append(unit)
        return self._unary_operation_handler(
            db2gse_function = 'DB2GSE.ST_AREA',
            valid_types = ['ST_GEOMETRY'],
            additional_args = additional_args)

    def dimension(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry.

        Returns an IdaSeries with integers representing the dimension of each
        of the geometries in the calling IdaGeoSeries.

        If the given geometry is empty, then -1 is returned.
        For points and multipoints, the dimension is 0 (zero).
        For curves and multicurves, the dimension is 1.
        For polygons and multipolygons, the dimension is 2.

        For None geometries the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        DB2 Spatial Extender ST_DIMENSION() function.

        Examples
        --------
        # TODO add examples
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_DIMENSION',
                valid_types = ['ST_GEOMETRY'])

    def length(self, unit = None):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_LINESTRING, ST_MULTILINESTRING.

        Returns an IdaSeries with doubles representing the length of each of
        the curves or multicurves in the calling IdaGeoSeries, in the given
        unit or else in the default unit.

        For None curves or multicurves the output is None.
        For empty curves or multicurves the output is None.

        Parameters
        ----------
        unit : str, optional
            Name of the unit, it is case-insensitive.
            If omitted, the following rules are used:
                * If curve is in a projected or geocentric coordinate system,
                the linear unit associated with this coordinate system is the
                default.
                * If curve is in a geographic coordinate system, the angular
                unit associated with this coordinate system is the default.

        Returns
        -------
        IdaSeries.

        See also
        --------
        linear_units : list of valid units.

        Notes
        -----
        Restrictions on unit conversions: An error (SQLSTATE 38SU4) is returned
        if any of the following conditions occur:
            * The curve is in an unspecified coordinate system and the unit
            parameter is specified.
            * The curve is in a projected coordinate system and an angular unit
            is specified.
            * The curve is in a geographic coordinate system, and a linear unit
            is specified.
        # TODO: handle this SQLSTATE error

        References
        ----------
        DB2 Spatial Extender ST_LENGTH() function.

        Examples
        --------
        >>> tornadoes = IdaGeoDataFrame(idadb,'SAMPLES.GEO_TORNADO',indexer='OBJECTID')
        >>> tornadoes.set_geometry('SHAPE')
        >>> tornadoes['length'] = tornadoes.length(unit = 'KILOMETER')
        >>> tornadoes[['OBJECTID', 'SHAPE', 'length']].head()
        OBJECTID    SHAPE                                              length
        1           MULTILINESTRING ((-90.2200062071 38.7700071663..   17.798545
        2           MULTILINESTRING ((-89.3000059755 39.1000072739...  6.448745
        3           MULTILINESTRING ((-84.5800047496 40.8800078382...  0.014213
        4           MULTILINESTRING ((-94.3700070010 34.4000061520..   0.014173
        5           MULTILINESTRING ((-90.6800062393 37.6000069289..   4.254681
        """
        additional_args = []
        if unit is not None:
            unit = self._check_linear_unit(unit)  # Can raise exceptions
            additional_args.append(unit)
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_LENGTH',
                valid_types = ['ST_LINESTRING', 'ST_MULTILINESTRING'],
                additional_args = additional_args)

    def perimeter(self, unit = None):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_POLYGON, ST_MULTIPOLYGON.

        Returns an IdaSeries with doubles representing the perimeter of each of
        the surfaces or multisurfaces in the calling IdaGeoSeries, in the given
        unit or else in the default unit.

        For None curves or multicurves the output is None.
        For empty curves or multicurves the output is None.

        Parameters
        ----------
        unit : str, optional
            Name of the unit, it is case-insensitive.
            If omitted, the following rules are used:
                * If surface is in a projected or geocentric coordinate system,
                the linear unit associated with this coordinate system is the
                default.
                * If surface is in a geographic coordinate system, the angular
                unit associated with this coordinate system is the default.

        Returns
        -------
        IdaSeries.

        See also
        --------
        linear_units : list of valid units.

        Notes
        -----
        Restrictions on unit conversions: An error (SQLSTATE 38SU4) is returned
        if any of the following conditions occur:
            * The geometry is in an unspecified coordinate system and the unit
            parameter is specified.
            * The geometry is in a projected coordinate system and an angular
            unit is specified.
            * The geometry is in a geographic coordinate system and a linear
            unit is specified.
        # TODO: handle this SQLSTATE error

        References
        ----------
        DB2 Spatial Extender ST_PERIMETER() function.

        Examples
        --------
        # TODO add examples
        """
        additional_args = []
        if unit is not None:
            unit = self._check_linear_unit(unit)  # Can raise exceptions
            additional_args.append(unit)
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_PERIMETER',
                valid_types = ['ST_POLYGON', 'ST_MULTIPOLYGON'],
                additional_args = additional_args)

    def num_geometries(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_MULTIPOINT, ST_MULTIPOLYGON, ST_MULTILINESTRING.

        Returns an IdaSeries with integers representing the number of
        geometries in each of the collections in the calling IdaGeoSeries.

        For None collections the output is None.
        For empty collections the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        DB2 Spatial Extender ST_NUMGEOMETRIES() function.

        Examples
        --------
        # TODO add examples
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_NUMGEOMETRIES',
                valid_types = ['ST_MULTIPOINT', 'ST_MULTIPOLYGON',
                               'ST_MULTILINESTRING'])

    def num_interior_ring(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_POLYGON.

        Returns an IdaSeries with integers representing the number of interior
        rings of each of the polygons in the calling IdaGeoSeries.

        For None collections the output is None.
        For empty collections the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        DB2 Spatial Extender ST_NUMINTERIORRING() function.

        Examples
        --------
        # TODO add examples
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_NUMINTERIORRING',
                valid_types = ['ST_POLYGON'])

    def num_line_strings(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_MULTILINESTRING.

        Returns an IdaSeries with integers representing the number of
        linestrings of each of the multilinestrings in the calling
        IdaGeoSeries.

        For None multilinestrings the output is None.
        For empty multilinestrings the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        DB2 Spatial Extender ST_NUMLINESTRINGS() function.

        Examples
        --------
        # TODO add examples
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_NUMLINESTRINGS',
                valid_types = ['ST_MULTILINESTRING'])

    def num_points(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with integers representing the number of points of
        each of the geometries in the calling IdaGeoSeries.

        For None geometries the output is None.
        For empty geometries the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        DB2 Spatial Extender ST_NUMPOINTS() function.

        Examples
        --------
        # TODO add examples
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_NUMPOINTS',
                valid_types = ['ST_GEOMETRY'])

    def num_polygons(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_MULTIPOLYGON.

        Returns an IdaSeries with integers representing the number of
        polygons of each of the multipolygons in the calling IdaGeoSeries.

        For None multipolygons the output is None.
        For empty multipolygons the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        DB2 Spatial Extender ST_NUMLINESTRINGS() function.

        Examples
        --------
        # TODO add examples
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_NUMPOLYGONS',
                valid_types = ['ST_MULTIPOLYGON'])

    def coord_dim(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with integers representing the dimensionality of
        the coordinates of each of the geometries in the calling IdaGeoSeries.

        If the given geometry does not have Z and M coordinates,
        the dimensionality is 2.
        If it has Z coordinates and no M coordinates, or if it has M
        coordinates and no Z coordinates, the dimensionality is 3.
        If it has Z and M coordinates, the dimensionality is 4.

        For None geometries the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        DB2 Spatial Extender ST_COORDDIM() function.

        Examples
        --------
        # TODO add examples
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_COORDDIM',
                valid_types = ['ST_GEOMETRY'])

    def is_3d(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with integers (1 if it has Z coordiantes, 0
        otherwise) for each of the geometries in the calling IdaGeoSeries.

        For None geometries the output is None.
        For empty geometries the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        DB2 Spatial Extender ST_IS3D() function.

        Examples
        --------
        # TODO add examples
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_IS3D',
                valid_types = ['ST_GEOMETRY'])

    def is_measured(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with integers (1 if it has M coordiantes, 0
        otherwise) for each of the geometries in the calling IdaGeoSeries.

        For None geometries the output is None.
        For empty geometries the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        DB2 Spatial Extender ST_ISMEASURED() function.

        Examples
        --------
        # TODO add examples
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_ISMEASURED',
                valid_types = ['ST_GEOMETRY'])

    def is_valid(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with integers (1 if it is valid, 0 otherwise) for
        each of the geometries in the calling IdaGeoSeries.

        A geometry is valid only if all of the attributes in the structured
        type are consistent with the internal representation of geometry data,
        and if the internal representation is not corrupted.

        For None geometries the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        DB2 Spatial Extender ST_ISVALID() function.

        Examples
        --------
        # TODO add examples
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_ISVALID',
                valid_types = ['ST_GEOMETRY'])

    def max_m(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with doubles representing the maximum M coordinate
        for each of the geometries in the calling IdaGeoSeries.

        For None geometries the output is None.
        For empty geometries the output is None.
        For geometries without M coordinate the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        DB2 Spatial Extender ST_MAXM() function.

        Examples
        --------
        # TODO add examples
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_MAXM',
                valid_types = ['ST_GEOMETRY'])

    def max_x(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with doubles representing the maximum X coordinate
        for each of the geometries in the calling IdaGeoSeries.

        For None geometries the output is None.
        For empty geometries the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        DB2 Spatial Extender ST_MAXX() function.

        Examples
        --------
        # TODO add examples
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_MAXX',
                valid_types = ['ST_GEOMETRY'])

    def max_y(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with doubles representing the maximum Y coordinate
        for each of the geometries in the calling IdaGeoSeries.

        For None geometries the output is None.
        For empty geometries the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        DB2 Spatial Extender ST_MAXY() function.

        Examples
        --------
        # TODO add examples
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_MAXY',
                valid_types = ['ST_GEOMETRY'])

    def max_z(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with doubles representing the maximum Z coordinate
        for each of the geometries in the calling IdaGeoSeries.

        For None geometries the output is None.
        For empty geometries the output is None.
        For geometries without Z coordinate the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        DB2 Spatial Extender ST_MAXZ() function.

        Examples
        --------
        # TODO add examples
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_MAXZ',
                valid_types = ['ST_GEOMETRY'])

    def min_m(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with doubles representing the minimum M coordinate
        for each of the geometries in the calling IdaGeoSeries.

        For None geometries the output is None.
        For empty geometries the output is None.
        For geometries without M coordinate the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        DB2 Spatial Extender ST_MINM() function.

        Examples
        --------
        # TODO add examples
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_MINM',
                valid_types = ['ST_GEOMETRY'])

    def min_x(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with doubles representing the minimum X coordinate
        for each of the geometries in the calling IdaGeoSeries.

        For None geometries the output is None.
        For empty geometries the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        DB2 Spatial Extender ST_MINX() function.

        Examples
        --------
        # TODO add examples
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_MINX',
                valid_types = ['ST_GEOMETRY'])

    def min_y(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with doubles representing the minimum Y coordinate
        for each of the geometries in the calling IdaGeoSeries.

        For None geometries the output is None.
        For empty geometries the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        DB2 Spatial Extender ST_MINY() function.

        Examples
        --------
        # TODO add examples
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_MINY',
                valid_types = ['ST_GEOMETRY'])

    def min_z(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with doubles representing the minimum Z coordinate
        for each of the geometries in the calling IdaGeoSeries.

        For None geometries the output is None.
        For empty geometries the output is None.
        For geometries without Z coordinate the output is None.
       
        Returns
        -------
        IdaSeries.

        References
        ----------
        DB2 Spatial Extender ST_MINZ() function.

        Examples
        --------
        # TODO add examples
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_MINZ',
                valid_types = ['ST_GEOMETRY'])

    def m(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_POINT.

        Returns an IdaSeries with doubles representing the measure (M)
        coordinate of each of the points in the calling IdaGeoSeries.

        For None points the output is None.
        For empty points the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        DB2 Spatial Extender ST_M() function.

        Examples
        --------
        # TODO add examples
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_M',
                valid_types = ['ST_POINT'])

    def x(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_POINT.

        Returns an IdaSeries with doubles representing the X coordinate of each
        of the points in the calling IdaGeoSeries.

        For None points the output is None.
        For empty points the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        DB2 Spatial Extender ST_X() function.

        Examples
        --------
        # TODO add examples
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_X',
                valid_types = ['ST_POINT'])

    def y(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_POINT.

        Returns an IdaSeries with doubles representing the Y coordinate of each
        of the points in the calling IdaGeoSeries.

        For None points the output is None.
        For empty points the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        DB2 Spatial Extender ST_Y() function.

        Examples
        --------
        # TODO add examples
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_Y',
                valid_types = ['ST_POINT'])

    def z(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_POINT.

        Returns an IdaSeries with doubles representing the Z coordinate of each
        of the points in the calling IdaGeoSeries.

        For None points the output is None.
        For empty points the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        DB2 Spatial Extender ST_Z() function.

        Examples
        --------
        # TODO add examples
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_Z',
                valid_types = ['ST_POINT'])

    def is_closed(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_LINESTRING, ST_MULTILINESTRING.

        Returns an IdaSeries with integers (1 if it is closed, 0 otherwise) for
        each of the curves or multicurves in the calling IdaGeoSeries.

        A curve is closed if the start point and end point are equal.
        If the curve has Z coordinates, the Z coordinates of the start and end
        point must be equal. Otherwise, the points are not considered equal,
        and the curve is not closed.
        A multicurve is closed if each of its curves are closed.

        For None curves or multicurves the output is None.
        For empty curves or multicurves the output is 0.

        Returns
        -------
        IdaSeries.

        References
        ----------
        DB2 Spatial Extender ST_ISCLOSED() function.

        Examples
        --------
        # TODO add examples
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_ISCLOSED',
                valid_types = ['ST_LINESTRING', 'ST_MULTILINESTRING'])

    def is_empty(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with integers (1 if it is empty, 0 otherwise) for
        each of the geometries in the calling IdaGeoSeries.

        For None geometries the output is None.

        Returns
        -------
        IdaSeries.

        References
        ----------
        DB2 Spatial Extender ST_ISEMPTY() function.

        Examples
        --------
        # TODO add examples
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_ISEMPTY',
                valid_types = ['ST_GEOMETRY'])

    def is_simple(self):
        """
        Valid types for the column in the calling IdaGeoSeries:
        ST_Geometry or one of its subtypes.

        Returns an IdaSeries with integers (1 if it is simple, 0 otherwise) for
        each of the geometries in the calling IdaGeoSeries.

        Points, surfaces, and multisurfaces are always simple.
        A curve is simple if it does not pass through the same point twice.
        Amultipoint is simple if it does not contain two equal points.
        A multicurve is simple if all of its curves are simple and the only
        intersections occur at points that are on the boundary of the curves in
        the multicurve.

        For None geometries the output is None.
        For empty geometries the output is 1.

        Returns
        -------
        IdaSeries.

        References
        ----------
        DB2 Spatial Extender ST_ISSIMPLE() function.

        Examples
        --------
        # TODO add examples
        """
        return self._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_ISSIMPLE',
                valid_types = ['ST_GEOMETRY'])


#==============================================================================
### Public utilities for geospatial methods
#==============================================================================

    @lazy
    def linear_units(self):
        units = self.ida_query(
            'SELECT UNIT_NAME FROM DB2GSE.ST_UNITS_OF_MEASURE WHERE '
            'UNIT_TYPE= \'LINEAR\' ORDER BY LENGTH(UNIT_NAME), UNIT_NAME')
        return units

#==============================================================================
### Private utilities for geospatial methods
#==============================================================================

    def _check_linear_unit(self, unit):
        """
        Parameters:
        -----------
        unit : str
            Name of a user-entered unit (case-insensitive).

        Returns
        -------
        str
            The name of the unit in uppercase and formatted for DB2GSE syntax.

        Raises
        ------
        TypeError
            * If the unit is not a string
            * If the unit is a string larger than 128 characters
        IdaGeoDataFrameError
            If the given unit is not a valid linear unit of DB2GSE.
        """

        if not isinstance(unit, six.string_types):
            raise TypeError("unit must be a string")
        elif len(unit) > 128:
            raise TypeError("unit length exceeded")
        else:
            unit = unit.upper()
            if unit not in self.linear_units.tolist():
                raise IdaGeoDataFrameError(
                    "Invalid unit\n"
                    "Hint: use linear_units attribute to see the valid units")
            else:
                # Replace single quotation marks with two of them
                if "\'" in unit:
                    unit = unit.replace("'", "''")

                # Enclose in single quotation marks
                unit = '\''+unit+'\''
                return unit

    def _unary_operation_handler(self, db2gse_function,
                                 valid_types,
                                 additional_args = None):
        """
        Returns the resulting column of an unary geospatial method as an
        IdaGeoSeries if it has geometry type, as an IdaSeries otherwise.

        Parameters
        ----------
        db2gse_function : str
            Name of the corresponding DB2GSE function.
        valid_types : list of str
            Valid input typenames.
        additional_args : list of str, optional
            Additional arguments for the DB2GSE function.

        Returns
        -------
        IdaGeoSeries
            If the resulting column has geometry type.
        IdaSeries
            If the resulting column doesn't have geometry type.
        """
        if not (self.dtypes.TYPENAME[0] in valid_types or 
                valid_types[0] == 'ST_GEOMETRY'):
            raise TypeError("Column " + self.column +
                            " has incompatible type.")

        # Obtain an IdaSeries object by cloning current one
        # Then modify its attribute column
        idaseries = self._clone()

        # Get the first argument for the DB2GSE function, i.e. a column.
        # Because it might be a non-destructive column that was added by the
        # user, the column definition is considered, instead of its alias
        # in the Ida object.
        column_for_db2gse = self.internal_state.columndict[self.column]
        if column_for_db2gse[0] == '\"' and column_for_db2gse[-1] == '\"':
                column_for_db2gse = column_for_db2gse[1:-1]

        arguments_for_db2gse_function = []
        arguments_for_db2gse_function.append(column_for_db2gse)

        if additional_args is not None:
            for arg in additional_args:
                arguments_for_db2gse_function.append(arg)

        result_column = (
            db2gse_function +
            '(' +
            ','.join(map(str, arguments_for_db2gse_function)) +
            ')'
            )

        new_columndict = OrderedDict()
        new_columndict[result_column] = result_column

        idaseries._reset_attributes(["columns", "shape", "dtypes"])
        idaseries.internal_state.columns = ['\"' + result_column + '\"']

        idaseries.internal_state.columndict = new_columndict
        idaseries.internal_state.update()

        # Set the column attribute of the new idaseries
        idaseries.column = result_column
        
        try:
            del(idaseries.columns)
        except:
            pass
        
        if idaseries.dtypes.TYPENAME[result_column].find('ST_') == 0:
            return IdaGeoSeries.from_IdaSeries(idaseries)
        else:
            return idaseries
