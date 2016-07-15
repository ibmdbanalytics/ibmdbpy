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
from builtins import zip
from builtins import str
from builtins import int
from future import standard_library
standard_library.install_aliases()

import ibmdbpy
from ibmdbpy.frame import IdaDataFrame
from ibmdbpy.geoSeries import IdaGeoSeries

from copy import  deepcopy

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
        Creates an IdaGeoDataFrame from an IdaDataFrame (or an IdaGeoDataFrame)
        
        Parameters
        ----------
        geometry : str, optional
            Name of the column to be set as "geometry" column of the
            IdaGeoDataFrame. It must have geometry type.

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

