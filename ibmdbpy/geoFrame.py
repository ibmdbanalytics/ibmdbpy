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
IdaGeoDataFrame
"""

# Ensure Python 2 compatibility
from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals
from __future__ import absolute_import
from builtins import super
from builtins import zip
from builtins import str
from builtins import int
from future import standard_library
standard_library.install_aliases()

import ibmdbpy
from ibmdbpy.frame import IdaDataFrame
from ibmdbpy.geoSeries import IdaGeoSeries

from copy import deepcopy

import six


class IdaGeoDataFrame(IdaDataFrame):
    """  
    An IdaGeoDataFrame container inherits from IdaDataFrame.

    It has a property called "geometry" which refers to a column with
    geometry type. It is set as a string with a column name, either at
    instantiation time or with the set_geometry() method.

    If the "geometry" property is set, when calling a geospatial method from
    IdaDataFrame the method will be carried on the column this property refers
    to.

    The property "geometry" returns an IdaGeoSeries.

    See IdaDataFrame.
    See IdaGeoSeries.

    Examples
    --------
    >>> idageodf = IdaGeoDataFrame(idadb, 'SAMPLES.GEO_COUNTY',
    indexer='OBJECTID')
    >>> idageodf.dtypes
                     TYPENAME
    OBJECTID          INTEGER
    SHAPE     ST_MULTIPOLYGON
    STATEFP           VARCHAR
    COUNTYFP          VARCHAR
    COUNTYNS          VARCHAR
    NAME              VARCHAR
    GEOID             VARCHAR
    NAMELSAD          VARCHAR
    LSAD              VARCHAR
    CLASSFP           VARCHAR
    MTFCC             VARCHAR
    CSAFP             VARCHAR
    CBSAFP            VARCHAR
    METDIVFP          VARCHAR
    FUNCSTAT          VARCHAR
    ALAND             DECIMAL
    AWATER            DECIMAL
    INTPTLAT          VARCHAR
    INTPTLON          VARCHAR

    >>> idageodf[['NAME', 'SHAPE']].head()
           NAME                                              SHAPE
    0    Becker  MULTIPOLYGON (((-95.1637185512 46.7176480983, ...
    1  Jim Hogg  MULTIPOLYGON (((-98.9542377853 26.7856984795, ...
    2     Henry  MULTIPOLYGON (((-88.0532984194 36.4970648458, ...
    3     Keith  MULTIPOLYGON (((-102.0517705602 41.0038968011,...
    4   Clinton  MULTIPOLYGON (((-94.2059683962 39.7458481141, ...

    >>> idageodf.geometry
    AttributeError: Geometry property has not been set yet. Use set_geometry
    method to set it.

    >>> idageodf.set_geometry('SHAPE')
    >>> idageodf.geometry.column
    'SHAPE'

    >>> type(idageodf.geometry)
    <class 'ibmdbpy.geoSeries.IdaGeoSeries'>

    >>> idageoseries = idageodf.geometry    
    >>> idageoseries.head()
    0    MULTIPOLYGON (((-95.1637185512 46.7176480983, ...
    1    MULTIPOLYGON (((-98.9542377853 26.7856984795, ...
    2    MULTIPOLYGON (((-88.0532984194 36.4970648458, ...
    3    MULTIPOLYGON (((-102.0517705602 41.0038968011,...
    4    MULTIPOLYGON (((-94.2059683962 39.7458481141, ...
    Name: SHAPE, dtype: object

    >>> idageodf['County area'] = idageodf.area(unit='mile')

    >>> counties_with_areas = idageodf[['NAME', 'SHAPE', 'County area']]
    >>> counties_with_areas.dtypes
                        TYPENAME
    NAME                 VARCHAR
    SHAPE        ST_MULTIPOLYGON
    County area           DOUBLE

    >>> counties_with_areas.head()
            NAME                                             SHAPE  County area
    0     Menard  MULTIPOLYGON (((-99.4847630885 30.940610279, ...   902.281540
    1      Boone  MULTIPOLYGON (((-88.7764991497 42.491919892, ...   282.045087
    2  Ochiltree  MULTIPOLYGON (((-100.5467326897 36.056542135,...   918.188142
    3    Sharkey  MULTIPOLYGON (((-90.9143429922 33.007703026, ...   435.548518
    4    Audubon  MULTIPOLYGON (((-94.7006367168 41.504155369, ...   444.827726
    """

    def __init__(self, idadb, tablename, indexer = None, geometry = None):
        """
        Constructor for IdaGeoDataFrame objects.
        See IdaDataFrame.__init__ documentation.

        Parameters
        ----------
        geometry : str, optional
            Column name to set the "geometry" property of the IdaGeoDataFrame.
            The column must have geometry type.

        Attributes
        ----------
        _geometry_colname : str
            Name of the column that "geometry" property refers to.
            This attribute must be set through the set_geometry() method.
        geometry : IdaGeoSeries
            The column referenced by _geometry_colname attribute.
        """
        # TODO: Add support for receiving either a string or an IdaGeoSeries as 
        # geometry parameter.        
        if geometry is not None and not isinstance(geometry, six.string_types):
            raise TypeError("geometry must be a string")
        super(IdaGeoDataFrame, self).__init__(idadb, tablename, indexer)
        self._geometry_colname = None
        if geometry is not None:
            self.set_geometry(geometry)

    def __getitem__(self, item):
        """
        Returns an IdaDataFrame, IdaSeries, IdaGeoDataFrame or IdaGeoSeries
        as appropriate.
        
        Returns
        --------
        IdaGeoSeries
            When the projection has only one column and it has geometry type.
        IdaGeoDataFrame
            When the projection has more than one column, and the "geometry"
            column of the IdaGeoDataFrame is included in them.
        IdaDataFrame
            When the projection has more than one column, and the "geometry"
            column of the IdaGeoDataFrame is not included in them.
        IdaSeries
            When the projection has only one column and it doesn't have 
            geometry type.
        """
        ida = super(IdaGeoDataFrame, self).__getitem__(item)
        if isinstance(ida, ibmdbpy.IdaSeries):
            if ida.dtypes['TYPENAME'][ida.column].find('ST_') == 0:
                idageoseries = IdaGeoSeries.from_IdaSeries(ida)
                # Return IdaGeoSeries
                return idageoseries
            else:
                # Return IdaSeries
                return ida
        elif isinstance(ida, ibmdbpy.IdaDataFrame):
            if self._geometry_colname in ida.dtypes.index:
                # Return IdaGeoDataFrame
                idageodf = IdaGeoDataFrame.from_IdaDataFrame(ida)
                idageodf._geometry_colname = self._geometry_colname
                return idageodf
            else:
                # Return IdaDataFrame
                return ida

    def __delitem__(self, item):
        """
        Erases the "geometry" property if the column it refers to is deleted.
        """
        super(IdaGeoDataFrame, self).__delitem__(item)
        if item == self._geometry_colname:
            self._geometry_colname = None

    def __getattr__(self, name):
        """
        Carry geospatial method calls on the "geometry" column of the
        IdaGeoDataFrame, if it was set.
        
        Notes
        -----
        This method gets called only when an attribute lookup on
        IdaGeoDataFrame is not resolved, i.e. it is not an instance attribute
        and it's not found in the class tree.
        """        
        
        if name == 'geometry':
            # When .geometry is accessed and _geometry_colname is None
            return self.__getattribute__('geometry')

        if hasattr(IdaGeoSeries, name):
            # Geospatial method call
            if self._geometry_colname is None:
                raise AttributeError("Geometry column has not been set yet.")
            else:
                # Get a IdaGeoSeries and carry the operation on it
                idageoseries = self.__getitem__(item = self._geometry_colname)
                return idageoseries.__getattribute__(name)
        else:
            raise AttributeError
    
    @property
    def geometry(self):
        """
        Returns an IdaGeoSeries with the column whose name is stored in 
        _geometry_colname attribute.

        The setter calls the set_geometry() method.

        Returns
        -------
        IdaGeoSeries

        Raises
        ------
        AttributeError
            If the property has not been set yet.
        
        """
        if self._geometry_colname is None:
            raise AttributeError(
                "Geometry property has not been set yet. "
                "Use set_geometry method to set it.")
        else:
            return self.__getitem__(self._geometry_colname)
    
    @geometry.setter
    def geometry(self, value):
        """
        See set_geometry() method.
        """
        self.set_geometry(value)

    @classmethod
    def from_IdaDataFrame(cls, idadf, geometry = None):
        """ 
        Creates an IdaGeoDataFrame from an IdaDataFrame.
        
        Parameters
        ----------
        geometry : str, optional
            Column name to set the "geometry" property of the IdaGeoDataFrame.
            The column must have geometry type.

        Raises
        ------
        TypeError
            If idadf is not an IdaDataFrame.
        """
        
        if not isinstance(idadf, IdaDataFrame):
            raise TypeError("Expected IdaDataFrame")
        else:
            # TODO: check if it's better to only change the .__base__ attribute

            #behavior based on _clone() method of IdaDataFrame
            newida = IdaGeoDataFrame(
                    idadf._idadb, idadf._name, idadf.indexer, geometry)
            newida.columns = idadf.columns 
            newida.dtypes = idadf.dtypes
            
            newida.internal_state.name = deepcopy(idadf.internal_state.name)
            newida.internal_state.ascending = deepcopy(idadf.internal_state.ascending)
            #newida.internal_state.views = deepcopy(idadf.internal_state.views)
            newida.internal_state._views = deepcopy(idadf.internal_state._views)
            newida.internal_state._cumulative = deepcopy(idadf.internal_state._cumulative)
            newida.internal_state.order = deepcopy(idadf.internal_state.order)
            newida.internal_state.columndict = deepcopy(idadf.internal_state.columndict)
            return newida

    def set_geometry(self, column_name):
        """
        Receives a column name to set as the "geometry" column of the
        IdaDataFrame.

        Parameters:
        -----------
        column_name : str
            Name of the column to be set as geometry column of the 
            IdaDataFrame. It must have geometry type.

        Raises
        ------
        KeyError
            If the column is not present in the IdaGeoDataFrame.
        TypeError
            If the column doesn't have geometry type.
        """
        if not isinstance(column_name, six.string_types):
            raise TypeError("column_name must be a string")
        if column_name not in self.columns:
            raise KeyError(
                "'" + column_name + "' cannot be set as geometry column: "
                "not a column in the IdaGeoDataFrame."
            )
        elif self.dtypes.TYPENAME[column_name].find('ST_') != 0:
            raise TypeError(
                "'" + column_name + "' cannot be set as geometry column: "
                "column doesn't have geometry type."
            )
        else:
            self._geometry_colname = column_name

    # ==============================================================================
    ### Binary geospatial methods
    # ==============================================================================
    def equals(self, ida2):
        """
        Valid types for the column in the calling IdaGeoDataFrame:
        ST_Geometry or one of its subtypes.

        Returns an IdaGeoDataFrame of indices of the two input
        IdaGeoDataFrames and a result column with 1 or 0 depending
        upon whether the geometry of the first IdaGeoDataFrame
        crosses the second.

        For None geometries the output is None.

        Returns
        -------
        Returns an IdaGeoDataFrame with three columns:

        INDEXERIDA1 : indexer of the first IdaGeoSeries (None if not set),
        INDEXERIDA2 : indexer of the second IdaGeoSeries (None if not set),
        RESULT : the result of the operation

        Parameters
        ----------
        ida2 : IdaGeoDataFrame
            Name of the second IdaGeoDataFrame on which the function ST_EQUALS()
            will be invoked.


        References
        ----------
        DB2 Spatial Extender ST_CROSSES() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>> ida1 = counties[counties['NAME'] == 'Austin']
        >>> ida2 = counties[counties['NAME'] == 'Kent']
        >>> result = ida1.equals(ida2)
        >>> result.head()
        INDEXERIDA1  INDEXERIDA2  RESULT
        2            163          0
        2            1840         0
        2            109          0
        """
        return self._binary_operation_handler(
            ida2,
            db2gse_function='DB2GSE.ST_EQUALS',
            valid_types_ida1=['ST_GEOMETRY'],
            valid_types_ida2=['ST_GEOMETRY'])

    def distance(self, ida2, unit=None):
        """
        Valid types for the column in the calling IdaGeoDataFrame:
        ST_Geometry or one of its subtypes.

        Returns an IdaGeoDataFrame of indices of the two input
        IdaGeoDataFrames and a result column with a numeric value
        which is the geographic distance measured between the
        geometries of the input IdaGeoDataFrames.

        For None geometries the output is None.

        Returns
        -------
        Returns an IdaGeoDataFrame with three columns:

        INDEXERIDA1 : indexer of the first IdaGeoSeries (None if not set),
        INDEXERIDA2 : indexer of the second IdaGeoSeries (None if not set),
        RESULT : the result of the operation

        Parameters
        ----------
        ida2 : IdaGeoDataFrame
            Name of the second IdaGeoDataFrame on which the function ST_EQUALS()
            will be invoked.
        unit : str, optional
            Name of the unit, it is case-insensitive.
            If omitted, the following rules are used:
                * If geometry is in a projected or geocentric coordinate
                system, the linear unit associated with this coordinate system
                is used.
                * If geometry is in a geographic coordinate system, the angular
                unit associated with this coordinate system is used.

        References
        ----------
        DB2 Spatial Extender ST_DISTANCE() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>> ida1 = counties[counties['NAME'] == 'Austin']
        >>> ida2 = counties[counties['NAME'] == 'Kent']
        >>> result = ida1.distance(ida2,unit = 'KILOMETER')
        >>> result.head()
        INDEXERIDA1  INDEXERIDA2  RESULT
        2            163          26.918942
        2            1840         4.868971
        2            109          16.387094
        """
        additional_args = []
        if unit is not None:
            unit = self._check_linear_unit(unit)  # Can raise exceptions
            additional_args.append(unit)
        return self._binary_operation_handler(
            ida2,
            db2gse_function='DB2GSE.ST_DISTANCE',
            valid_types_ida1=['ST_GEOMETRY'],
            valid_types_ida2=['ST_GEOMETRY'])

    def crosses(self, ida2):
        """
        Valid types for the column in the calling IdaGeoDataFrame:
        ST_Geometry or one of its subtypes.

        Returns an IdaGeoDataFrame of indices of the two input
        IdaGeoDataFrames and a result column with 1 or 0 depending
        upon whether the geometry of the first IdaGeoDataFrame
        crosses the second.

        For None geometries the output is None.

        Returns
        -------
        Returns an IdaGeoDataFrame with three columns:

        INDEXERIDA1 : indexer of the first IdaGeoSeries (None if not set),
        INDEXERIDA2 : indexer of the second IdaGeoSeries (None if not set),
        RESULT : the result of the operation

        Parameters
        ----------
        ida2 : IdaGeoDataFrame
            Name of the second IdaGeoDataFrame on which the function ST_EQUALS()
            will be invoked.

        References
        ----------
        DB2 Spatial Extender ST_CROSSES() function.

        See also
        --------
        linear_units : list of valid units.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>> ida1 = counties[counties['NAME'] == 'Austin']
        >>> ida2 = counties[counties['NAME'] == 'Kent']
        >>> result = ida1.crosses(ida2)
        >>> result.head()
        INDEXERIDA1  INDEXERIDA2  RESULT
        2            163          0
        2            1840         0
        2            109          0
        """
        return self._binary_operation_handler(
            ida2,
            db2gse_function='DB2GSE.ST_CROSSES',
            valid_types_ida1=['ST_GEOMETRY'],
            valid_types_ida2=['ST_GEOMETRY'])

    def intersects(self, ida2):
        """
        Valid types for the column in the calling IdaGeoDataFrame:
        ST_Geometry or one of its subtypes.

        Returns an IdaGeoDataFrame of indices of the two input
        IdaGeoDataFrames and a result column with 1 or 0 depending
        upon whether the geometries of the input IdaGeoDataFrames
        intersect each other.

        For None geometries the output is None.

        Returns
        -------
        Returns an IdaGeoDataFrame with three columns:

        INDEXERIDA1 : indexer of the first IdaGeoSeries (None if not set),
        INDEXERIDA2 : indexer of the second IdaGeoSeries (None if not set),
        RESULT : the result of the operation

        Parameters
        ----------
        ida2 : IdaGeoDataFrame
            Name of the second IdaGeoDataFrame on which the function ST_EQUALS()
            will be invoked.

        References
        ----------
        DB2 Spatial Extender ST_INTERSECTS() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>> ida1 = counties[counties['NAME'] == 'Austin']
        >>> ida2 = counties[counties['NAME'] == 'Kent']
        >>> result = ida1.intersects(ida2)
        >>> result.head()
        INDEXERIDA1  INDEXERIDA2  RESULT
        2            163          0
        2            1840         0
        2            109          0
        """
        return self._binary_operation_handler(
            ida2,
            db2gse_function='DB2GSE.ST_INTERSECTS',
            valid_types_ida1=['ST_GEOMETRY'],
            valid_types_ida2=['ST_GEOMETRY'])

    def overlaps(self, ida2):
        """
        Valid types for the column in the calling IdaGeoDataFrame:
        ST_Geometry or one of its subtypes.

        Returns an IdaGeoDataFrame of indices of the two input
        IdaGeoDataFrames and a result column with 1 or 0 depending
        upon whether the geometries of the input IdaGeoDataFrames
        overlap each other.

        For None geometries the output is None.

        Returns
        -------
        Returns an IdaGeoDataFrame with three columns:

        INDEXERIDA1 : indexer of the first IdaGeoSeries (None if not set),
        INDEXERIDA2 : indexer of the second IdaGeoSeries (None if not set),
        RESULT : the result of the operation


        Parameters
        ----------
        ida2 : IdaGeoDataFrame
            Name of the second IdaGeoDataFrame on which the function ST_EQUALS()
            will be invoked.

        References
        ----------
        DB2 Spatial Extender ST_OVERLAPS() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>> ida1 = counties[counties['NAME'] == 'Austin']
        >>> ida2 = counties[counties['NAME'] == 'Kent']
        >>> result = ida1.overlaps(ida2)
        >>> result.head()
        INDEXERIDA1  INDEXERIDA2  RESULT
        2            163          0
        2            1840         0
        2            109          0
        """
        return self._binary_operation_handler(
            ida2,
            db2gse_function='DB2GSE.ST_OVERLAPS',
            valid_types_ida1=['ST_GEOMETRY'],
            valid_types_ida2=['ST_GEOMETRY'])

    def touches(self, ida2):
        """
        Valid types for the column in the calling IdaGeoDataFrame:
        ST_Geometry or one of its subtypes.

        Returns an IdaGeoDataFrame of indices of the two input
        IdaGeoDataFrames and a result column with 1 or 0 depending
        upon whether the boundary of the first geometry touches
        the second.

        For None geometries the output is None.

        Returns
        -------
        Returns an IdaGeoDataFrame with three columns:

        INDEXERIDA1 : indexer of the first IdaGeoSeries (None if not set),
        INDEXERIDA2 : indexer of the second IdaGeoSeries (None if not set),
        RESULT : the result of the operation

        Parameters
        ----------
        ida2 : IdaGeoDataFrame
            Name of the second IdaGeoDataFrame on which the function ST_EQUALS()
            will be invoked.

        References
        ----------
        DB2 Spatial Extender ST_TOUCHES() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>> ida1 = counties[counties['NAME'] == 'Austin']
        >>> ida2 = counties[counties['NAME'] == 'Kent']
        >>> result = ida1.touches(ida2)
        >>> result.head()
        INDEXERIDA1  INDEXERIDA2  RESULT
        2            163          0
        2            1840         0
        2            109          0
        """
        return self._binary_operation_handler(
            ida2,
            db2gse_function='DB2GSE.ST_TOUCHES',
            valid_types_ida1=['ST_GEOMETRY'],
            valid_types_ida2=['ST_GEOMETRY'])

    def disjoint(self, ida2):
        """
        Valid types for the column in the calling IdaGeoDataFrame:
        ST_Geometry or one of its subtypes.

        Returns an IdaGeoDataFrame of indices of the two input
        IdaGeoDataFrames and a result column with 1 or 0 depending
        upon whether the geometries in the input dataframes are
        disjoint.

        For None geometries the output is None.

        Returns
        -------
        Returns an IdaGeoDataFrame with three columns:

        INDEXERIDA1 : indexer of the first IdaGeoSeries (None if not set),
        INDEXERIDA2 : indexer of the second IdaGeoSeries (None if not set),
        RESULT : the result of the operation

        Parameters
        ----------
        ida2 : IdaGeoDataFrame
            Name of the second IdaGeoDataFrame on which the function ST_EQUALS()
            will be invoked.

        References
        ----------
        DB2 Spatial Extender ST_DISJOINT() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>> ida1 = counties[counties['NAME'] == 'Austin']
        >>> ida2 = counties[counties['NAME'] == 'Kent']
        >>> result = ida1.disjoint(ida2)
        >>> result.head()
        INDEXERIDA1  INDEXERIDA2  RESULT
        2            163          1
        2            1840         1
        2            109          1
        """
        return self._binary_operation_handler(
            ida2,
            db2gse_function='DB2GSE.ST_DISJOINT',
            valid_types_ida1=['ST_GEOMETRY'],
            valid_types_ida2=['ST_GEOMETRY'])

    def contains(self, ida2):
        """
        Valid types for the column in the calling IdaGeoDataFrame:
        ST_Geometry or one of its subtypes.

        Returns an IdaGeoDataFrame of indices of the two input
        IdaGeoDataFrames and a result column with 1 or 0 depending
        upon whether the second geometry contains the first.

        For None geometries the output is None.

        Returns
        -------
        Returns an IdaGeoDataFrame with three columns:

        INDEXERIDA1 : indexer of the first IdaGeoSeries (None if not set),
        INDEXERIDA2 : indexer of the second IdaGeoSeries (None if not set),
        RESULT : the result of the operation

        Parameters
        ----------
        ida2 : IdaGeoDataFrame
            Name of the second IdaGeoDataFrame on which the function ST_EQUALS()
            will be invoked.

        References
        ----------
        DB2 Spatial Extender ST_CONTAINS() function.

        Examples
        --------
        >>> idageodf_customer = IdaGeoDataFrame(idadb,'SAMPLES.GEO_CUSTOMER',indexer='OBJECTID')
        >>> idageodf_customer.set_geometry('SHAPE')
        >>> idageodf_county = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> idageodf_county.set_geometry('SHAPE')
        >>> ida1 = idageodf_customer[idageodf_customer['INSURANCE_VALUE']>250000]
        >>> ida2 = idageodf_county[idageodf_county['NAME']=='Madison']
        >>> result = ida2.contains(ida1)
        >>> result[result['RESULT']==1].head()
        INDEXERIDA1    INDEXERIDA2    RESULT
        21473          134            1
        21413          134            1
        21414          134            1
        21417          134            1
        21419          134            1
        """
        return self._binary_operation_handler(
            ida2,
            db2gse_function='DB2GSE.ST_CONTAINS',
            valid_types_ida1=['ST_GEOMETRY'],
            valid_types_ida2=['ST_GEOMETRY'])

    def within(self, ida2):
        """
        Valid types for the column in the calling IdaGeoDataFrame:
        ST_Geometry or one of its subtypes.

        Returns an IdaGeoDataFrame of indices of the two input
        IdaGeoDataFrames and a result column with 1 or 0 depending
        upon whether the first geometry is inside the second.

        For None geometries the output is None.

        Returns
        -------
        Returns an IdaGeoDataFrame with three columns:

        INDEXERIDA1 : indexer of the first IdaGeoSeries (None if not set),
        INDEXERIDA2 : indexer of the second IdaGeoSeries (None if not set),
        RESULT : the result of the operation

        Parameters
        ----------
        ida2 : IdaGeoDataFrame
            Name of the second IdaGeoDataFrame on which the function ST_EQUALS()
            will be invoked.

        References
        ----------
        DB2 Spatial Extender ST_WITHIN() function.

        Examples
        --------
        >>> idageodf_customer = IdaGeoDataFrame(idadb,'SAMPLES.GEO_CUSTOMER',indexer='OBJECTID')
        >>> idageodf_customer.set_geometry('SHAPE')
        >>> idageodf_county = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> idageodf_county.set_geometry('SHAPE')
        >>> ida1 = idageodf_customer[idageodf_customer['INSURANCE_VALUE']>250000]
        >>> ida2 = idageodf_county[idageodf_county['NAME']=='Madison']
        >>> result = ida1.within(ida2)
        >>> result[result['RESULT']==1].head()
        INDEXERIDA1    INDEXERIDA2    RESULT
        134            21473          1
        134            21413          1
        134            21414          1
        134            21417          1
        134            21419          1
        """
        return self._binary_operation_handler(
            ida2,
            db2gse_function='DB2GSE.ST_WITHIN',
            valid_types_ida1=['ST_GEOMETRY'],
            valid_types_ida2=['ST_GEOMETRY'])

    def mbr_intersects(self, ida2):
        """
        This method takes a second IdaGeoDataFrame an an input
        and checks if the Minimum Bounding rectangles of the
        geometries from both IdaGeoDataFrames intersect and
        stores the result as 0 or 1 in the RESULT column of
        the resulting IdaGeoDataFrame.

        For None geometries the output is None.
        For empty geometries the output is None.

        Returns
        -------
        Returns an IdaGeoDataFrame with three columns:

        INDEXERIDA1 : indexer of the first IdaGeoSeries (None if not set),
        INDEXERIDA2 : indexer of the second IdaGeoSeries (None if not set),
        RESULT : the result of the operation

        Parameters
        ----------
        ida2 : IdaGeoDataFrame
            Name of the second IdaGeoDataFrame on which the function ST_EQUALS()
            will be invoked.

        References
        ----------
        DB2 Spatial Extender ST_MBRIntersects() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>> ida1 = counties[counties['NAME'] == 'Austin']
        >>> ida2 = counties[counties['NAME'] == 'Kent']
        >>> result = ida1.difference(ida2)
        >>> result.head()
        INDEXERIDA1  INDEXERIDA2  RESULT
        2            163          0
        2            1840         0
        2            109          0
        """
        return self._binary_operation_handler(
            ida2,
            db2gse_function='DB2GSE.ST_MBRINTERSECTS',
            valid_types_ida1=['ST_GEOMETRY'],
            valid_types_ida2=['ST_GEOMETRY'])

    def difference(self, ida2):
        """
        This method takes a second IdaGeoDataFrame an an input
        and returns the difference of the geometries from both
        IdaGeoDataFrames as a new geometry stored in the RESULT
        column of the resulting IdaGeoDataFrame.

        For None geometries the output is None.
        For empty geometries the output is None.

        Returns
        -------
        Returns an IdaGeoDataFrame with three columns:

        INDEXERIDA1 : indexer of the first IdaGeoSeries (None if not set),
        INDEXERIDA2 : indexer of the second IdaGeoSeries (None if not set),
        RESULT : the result of the operation

        Parameters
        ----------
        ida2 : IdaGeoDataFrame
            Name of the second IdaGeoDataFrame on which the function ST_EQUALS()
            will be invoked.

        References
        ----------
        DB2 Spatial Extender ST_Difference() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>> ida1 = counties[counties['NAME'] == 'Austin']
        >>> ida2 = counties[counties['NAME'] == 'Kent']
        >>> result = ida1.difference(ida2)
        >>> result.head()
        INDEXERIDA1  INDEXERIDA2  RESULT
        2            163          POLYGON ((-96.6219873342 30.0442882117, -96.61...
        2            1840         POLYGON ((-96.6219873342 30.0442882117, -96.61...
        2            109          POLYGON ((-96.6219873342 30.0442882117, -96.61...
        """
        return self._binary_operation_handler(
            ida2,
            db2gse_function='DB2GSE.ST_DIFFERENCE',
            valid_types_ida1=['ST_GEOMETRY'],
            valid_types_ida2=['ST_GEOMETRY'])

    def intersection(self, ida2):
        """
        This method takes a second IdaGeoDataFrame an an input
        and returns the intersection of the geometries from both
        IdaGeoDataFrames as a new geometry stored in the RESULT
        column of the resulting IdaGeoDataFrame.

        For None geometries the output is None.
        For empty geometries the output is POINT EMPTY.

        Returns
        -------
        Returns an IdaGeoDataFrame with three columns:

        INDEXERIDA1 : indexer of the first IdaGeoSeries (None if not set),
        INDEXERIDA2 : indexer of the second IdaGeoSeries (None if not set),
        RESULT : the result of the operation

        Parameters
        ----------
        ida2 : IdaGeoDataFrame
            Name of the second IdaGeoDataFrame on which the function ST_EQUALS()
            will be invoked.

        References
        ----------
        DB2 Spatial Extender ST_Intersection() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>> ida1 = counties[counties['NAME'] == 'Austin']
        >>> ida2 = counties[counties['NAME'] == 'Kent']
        >>> result = ida1.intersection(ida2)
        >>> result.head()
        INDEXERIDA1  INDEXERIDA2  RESULT
        2            163          POINT EMPTY
        2            1840         POINT EMPTY
        2            109          POINT EMPTY
        """
        return self._binary_operation_handler(
            ida2,
            db2gse_function='DB2GSE.ST_INTERSECTION',
            valid_types_ida1=['ST_GEOMETRY'],
            valid_types_ida2=['ST_GEOMETRY'])

    def union(self, ida2):
        """
        This method takes a second IdaGeoDataFrame an an input
        and returns the union of the geometries from both
        IdaGeoDataFrames as a new geometry stored in the RESULT
        column of the resulting IdaGeoDataFrame.

        For None geometries the output is None.
        For empty geometries the output is None.

        Returns
        -------
        Returns an IdaGeoDataFrame with three columns:

        INDEXERIDA1 : indexer of the first IdaGeoSeries (None if not set),
        INDEXERIDA2 : indexer of the second IdaGeoSeries (None if not set),
        RESULT : the result of the operation

        Parameters
        ----------
        ida2 : IdaGeoDataFrame
            Name of the second IdaGeoDataFrame on which the function ST_EQUALS()
            will be invoked.

        References
        ----------
        DB2 Spatial Extender ST_Union() function.

        Examples
        --------
        >>> counties = IdaGeoDataFrame(idadb,'SAMPLES.GEO_COUNTY',indexer='OBJECTID')
        >>> counties.set_geometry('SHAPE')
        >>> ida1 = counties[counties['NAME'] == 'Austin']
        >>> ida2 = counties[counties['NAME'] == 'Kent']
        >>> result = ida1.union(ida2)
        >>> result.head()
        INDEXERIDA1  INDEXERIDA2  RESULT
        2            163          MULTIPOLYGON (((-96.6219873342 30.0442882117, ...
        2            1840         MULTIPOLYGON (((-96.6219873342 30.0442882117, ...
        2            109          MULTIPOLYGON (((-96.6219873342 30.0442882117, ..
        """
        return self._binary_operation_handler(
            ida2,
            db2gse_function='DB2GSE.ST_UNION',
            valid_types_ida1=['ST_GEOMETRY'],
            valid_types_ida2=['ST_GEOMETRY'])

    def _binary_operation_handler(self, ida2, db2gse_function,
                                          valid_types_ida1, valid_types_ida2,
                                          additional_args=None):


        """
        Returns an IdaGeoDataFrame with three columns:
        [
        INDEXERIDA1 : indexer of the first IdaGeoSeries (None if not set),
        INDEXERIDA2 : indexer of the second IdaGeoSeries (None if not set),
        RESULT : the result of the operation
        ]


        Parameters
        ----------
        db2gse_function : str
                Name of the corresponding DB2GSE function.
        valid_types_ida1 : list of str
                Valid input typenames for the first IdaGeoSeries.
        valid_types_ida2 : list of str
                Valid input typenames for the second IdaGeoSeries.
        additional_args : list of str, optional
                Additional arguments for the DB2GSE function.

        Returns
        -------
        IdaGeoDataFrame
        """
        ida1 = self  # For code clearness
        if not (ida1.dtypes.TYPENAME[0] in valid_types_ida1 or
                        valid_types_ida1[0] == 'ST_GEOMETRY'):
            raise TypeError("Column " + ida1.column +
                            " has incompatible type.")
        if not (ida2.dtypes.TYPENAME[0] in valid_types_ida2 or
                        valid_types_ida2[0] == 'ST_GEOMETRY'):
            raise TypeError("Column " + ida1.column +
                            " has incompatible type.")

        # Get the definitions of the columns, which will be the arguments for
        # the DB2GSE function
        column1_for_db2gse = ida1.internal_state.columndict[self.geometry.column]
        if column1_for_db2gse[0] == '\"' and column1_for_db2gse[-1] == '\"':
            column1_for_db2gse = column1_for_db2gse[1:-1]
        column2_for_db2gse = ida2.internal_state.columndict[self.geometry.column]
        if column2_for_db2gse[0] == '\"' and column2_for_db2gse[-1] == '\"':
            column2_for_db2gse = column2_for_db2gse[1:-1]


        arguments_for_db2gse_function = []
        arguments_for_db2gse_function.append('IDA1.' + column1_for_db2gse)
        arguments_for_db2gse_function.append('IDA2.' + column1_for_db2gse)
        if additional_args is not None:
            for arg in additional_args:
                arguments_for_db2gse_function.append(arg)

        # SELECT statement
        select_columns = []
        if hasattr(ida1, '_indexer') and ida1._indexer is not None:
            select_columns.append('IDA1.\"%s\" AS \"INDEXERIDA1\"' % (ida1.indexer))
        else:
            message = (ida1 + "has no indexer defined. Please assign index column with set_indexer and retry.")
            raise IdaGeoDataFrameError(message)
        if hasattr(ida2, '_indexer') and ida2._indexer is not None:
            select_columns.append('IDA2.\"%s\" AS \"INDEXERIDA2\"' % (ida1.indexer))
        else:
            message = (ida2 + "has no indexer defined. Please assign index column with set_indexer and retry.")
            raise IdaGeoDataFrameError(message)
        result_column = (
            db2gse_function +
            '(' +
            ','.join(map(str, arguments_for_db2gse_function)) +
            ')'
        )
        select_columns.append('%s AS \"RESULT\"' % (result_column))
        select_statement = 'SELECT ' + ','.join(select_columns) + ' '

        # FROM clause
        from_clause = (
            'FROM ' +
            ida1.name + ' AS IDA1, ' +
            ida2.name + ' AS IDA2 '
        )

        # Create a view
        view_creation_query = '(' + select_statement + from_clause + ')'
        viewname = self._idadb._create_view_from_expression(
            view_creation_query)

        idageodf = ibmdbpy.IdaGeoDataFrame(self._idadb, viewname,indexer= 'INDEXERIDA1')
        return idageodf