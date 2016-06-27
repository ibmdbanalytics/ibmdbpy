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
---------
A IdaGeoDataFrame container inherits from IdaDataFrame and adds methods
for calling functions of DB2/DashDB Geospatial Extender (DB2GSE) in-database
    
See help of IdaDataFrame
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
from ibmdbpy.exceptions import IdaGeoDataFrameError

from numbers import Number
from copy import copy, deepcopy
from collections import OrderedDict

#external
from lazy import lazy
import six
import pandas as pd


class IdaGeoDataFrame(IdaDataFrame):
    """    
    A IdaGeoDataFrame container inherits from IdaDataFrame and adds wrapper
    methods for calling functions of DB2/DashDB Geospatial Extender (DB2GSE)
    (See IdaDataFrame).

    Examples
    --------
    >>> idadf = IdaGeoDataFrame(idadb, 'SAMPLES.GEO_TORNADO', indexer = 'OBJECTID'
    >>> idadf['tornado buffer'] = idadf.buffer(distance=5.2, unit='kilometer')
    >>> result = idadf[['OBJECTID','tornado buffer']]
    >>> result.head()
           OBJECTID                                     tornado buffer
    0         1  POLYGON ((-90.2424692061 38.8134250902, -90.24...
    1         2  POLYGON ((-89.3207378290 39.1439746422, -89.32...
    2         3  POLYGON ((-84.6178380905 40.9170006726, -84.62...
    3         4  POLYGON ((-94.4092880534 34.4337355958, -94.41...
    4         5  POLYGON ((-90.7261131703 37.6291587327, -90.72...
    """
    
    def __init__(self, idadb, tablename, indexer = None, geometry = None, verboseGeometryColumn = True):
        """
        Constructor for IdaGeoDataFrame objects (see IdaDataFrame class)
        
        Parameters
        ----------
        
        geometry : Optional[str]
            Name of the default column for geospatial operations.
            Geospatial methods will use this column as default if no column
            name was explicitly specified.
            If geometry parameter is None, the left-most geometry
            column (if any) of the IdaGeoDataFrame will be set as _geometry
            attribute. If there are no geometry columns in the IdaGeoDataFrame,
            _geometry attribute will be set to None.
            This attribute can be changed afterwards with the method setGeometry
        """
        super(IdaGeoDataFrame, self).__init__(idadb, tablename, indexer)
        if geometry is not None:
            geometry = geometry.upper()
            self.setGeometry(geometry) #will raise an exception as appropriate
        else:
            self._setGeometryAutomatically(verbose = verboseGeometryColumn)
    
    @classmethod
    def fromIdaDataFrame(self, idadf = None, geometry = None):
        """
        "Alternate" constructor. 
        Create an IdaGeoDataFrame from an IdaDataFrame
        
        Parameters
        ----------
        
        geometry : Optional[str]
            Name of the default column for geospatial operations.
            Geospatial methods will use this column as default if no column
            name was explicitly specified.
            If geometry parameter is None, the left-most geometry
            column (if any) of the IdaGeoDataFrame will be set as _geometry
            attribute. If there are no geometry columns in the IdaGeoDataFrame,
            _geometry attribute will be set to None.
            This attribute can be changed afterwards with the method setGeometry
        """
        
        if not isinstance(idadf, IdaDataFrame):
            raise TypeError("Expected IdaDataFrame")
        else:
            #behavior based on _clone() method
            newida = IdaGeoDataFrame(idadf._idadb, idadf._name, idadf.indexer, verboseGeometryColumn=False)
            newida.columns = idadf.columns 
            newida.dtypes = idadf.dtypes     # avoid recomputing it 
            # otherwise risk of infinite loop between 
            # idadf.columns and internalstate.columndict
            
            # This is not possible to use deepcopy on an IdaDataFrame object
            # because the reference to the parents IdaDataBase with the connection
            # object is not pickleable. As a consequence, we create a new
            # IdaDataFrame and copy all the relevant attributes
            
            newida.internal_state.name = deepcopy(idadf.internal_state.name)
            newida.internal_state.ascending = deepcopy(idadf.internal_state.ascending)
            #newida.internal_state.views = deepcopy(idadf.internal_state.views)
            newida.internal_state._views = deepcopy(idadf.internal_state._views)
            newida.internal_state._cumulative = deepcopy(idadf.internal_state._cumulative)
            newida.internal_state.order = deepcopy(idadf.internal_state.order)
            newida.internal_state.columndict = deepcopy(idadf.internal_state.columndict)
    
            #set the _geometry attribute
            if geometry is not None:
                geometry = geometry.upper()
                newida.setGeometry(geometry) #will raise an exception as appropriate
            else:
                newida._setGeometryAutomatically(verbose = True)
            
            return newida
                
#==============================================================================
### Attributes & Metadata computation
#==============================================================================

    def __getitem__(self, item):
        """
        Overriden method
        Differences with method of parent class:
        * when cloning, clones as IdaGeoDataFrame instead of as idaSeries
        * for column projection, sets the _geometry attribute of the resulting
          IdaGeoDataFrame
        
        See IdaDataFrame.__getitem__
        """
        if isinstance(item, ibmdbpy.filtering.FilterQuery):
            newidadf = self._clone()
            newidadf.internal_state.update(item)
            newidadf._reset_attributes(["shape"])
        else:
            if isinstance(item, slice):
                result = self.loc[item]
                result._setGeometryAutomatically(verbose=True)
            if not (isinstance(item,six.string_types)|isinstance(item, list)):
                raise KeyError(item)
            if isinstance(item, six.string_types):
                #Case when only one column was selected
                if item not in self.columns:
                    raise KeyError(item)
                
                newidadf = self._clone()

                #form the new columndict
                for column in list(newidadf.internal_state.columndict):
                    if column != item:
                        del newidadf.internal_state.columndict[column]
                newColumndict = newidadf.internal_state.columndict
                
                #erase attributes
                newidadf._reset_attributes(["columns", "shape", "dtypes"])
                #set columns and columndict attributes
                newidadf.internal_state.columns = ["\"%s\""%col for col in item]
                newidadf.internal_state.columndict = newColumndict
                #update, i.e. appends an entry to internal_state._cumulative
                newidadf.internal_state.update()
                                
                # Performance improvement 
                newidadf.dtypes = self.dtypes.loc[[item]]
                
                newidadf._setGeometryAutomatically(verbose=False)
                return newidadf

            #Case when where multiple columns were selected                
            not_a_column = [x for x in item if x not in self.columns]
            if not_a_column:
                raise KeyError("%s"%not_a_column)            
            
            newidadf = self._clone()
                        
            #form the new columndict
            newColumndict = OrderedDict()            
            for col in item:
                #column name as key, its definition as value
                newColumndict[col] = self.internal_state.columndict[col]
                
            #erase attributes
            newidadf._reset_attributes(["columns", "shape", "dtypes"])
            #set columns and columndict attributes
            newidadf.internal_state.columns = ["\"%s\""%col for col in item]
            newidadf.internal_state.columndict = newColumndict
            #update, i.e. appends an entry to internal_state._cumulative
            newidadf.internal_state.update()
            
            # Performance improvement 
            newidadf.dtypes = self.dtypes.loc[item]
            
            newidadf._setGeometryAutomatically(verbose=True)

        return newidadf

#==============================================================================
#### Private methods
#==============================================================================

    def _clone(self):
            """
            Overriden method from IdaDataFrame. The only difference is that
            this one returns an IdaGeoDataFrame instead of an IdaDataFrame
            
            This is important for column projection because doing it using
            the _clone() method of IdaDataFrame would end up in having an
            IdaDataFrame instead of an IdaGeoDataFrame
            
            See IdaDataFrame._clone
            """
            #newida = IdaDataFrame(self._idadb, self._name, self.indexer)
            newida = IdaGeoDataFrame(self._idadb, self._name, self.indexer, verboseGeometryColumn=False)
            newida.columns = self.columns 
            newida.dtypes = self.dtypes     # avoid recomputing it 
            # otherwise risk of infinite loop between 
            # idadf.columns and internalstate.columndict
            
            # This is not possible to use deepcopy on an IdaDataFrame object
            # because the reference to the parents IdaDataBase with the connection
            # object is not pickleable. As a consequence, we create a new
            # IdaDataFrame and copy all the relevant attributes
            
            newida.internal_state.name = deepcopy(self.internal_state.name)
            newida.internal_state.ascending = deepcopy(self.internal_state.ascending)
            #newida.internal_state.views = deepcopy(self.internal_state.views)
            newida.internal_state._views = deepcopy(self.internal_state._views)
            newida.internal_state._cumulative = deepcopy(self.internal_state._cumulative)
            newida.internal_state.order = deepcopy(self.internal_state.order)
            newida.internal_state.columndict = deepcopy(self.internal_state.columndict)
            return newida
            
#==============================================================================
### Geospatial methods
#==============================================================================

    def buffer(self, colx=None, distance=None, unit=None):
        """
        This function takes a geometry column of the IdaGeoDataFrame, a distance, and, optionally, a unit as
        input parameters and returns the geometry that surrounds the given geometry by the specified distance,
        measured in the given unit. Each point on the boundary of the resulting geometry is
        the specified distance away from the given geometry.
        The resulting geometry is represented in the spatial reference system of the given geometry.

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame for which the buffer is to be computed.

        distance : A DOUBLE precision value that specifies the distance to be used for the buffer around geometry.

        unit : A VARCHAR(128) value that identifies the unit in which distance is measured. The supported units of measure
        are listed in the DB2GSE.ST_UNITS_OF_MEASURE catalog view.

        Returns
        -------
        It returns a new column in the IdaGeoDataFrame with the geometry of type ST_Polygon or ST_MultiPolygon.

        Examples
        --------
        >>> idadf = IdaGeoDataFrame(idadb, 'SAMPLES.GEO_TORNADO', indexer = 'OBJECTID')
        >>> idadf['Buffer_200_miles'] = idadf.buffer(colx = 'SHAPE', distance= 200, unit= 'STATUTE MILE')
            OBJECTID      DATE             TIME         LEN    WID        SHAPE                                              Buffer_200_miles
              115     1950-05-24         13:30:00       3.6    77         MULTILINESTRING ((-99.4800085321 37.2700063950...  POLYGON ((-100.2516146636 40.1057433898, -100....
              116     1950-05-24         17:30:00       10.7   40         MULTILINESTRING ((-96.9700080939 39.3800068879...  POLYGON ((-98.6396004401 41.9851176508, -98.86...
              117     1950-05-24         21:30:00       1.0    33         MULTILINESTRING ((-99.0300083486 36.4500062846...  POLYGON ((-101.7207742393 38.4011710646, -101....
              118     1950-05-25         16:30:00       3.0    880        MULTILINESTRING ((-101.8800087620 32.670005493...  POLYGON ((-99.7001980184 34.9305575445, -99.88...
              119     1950-05-29         14:48:00       1.0    33         MULTILINESTRING ((-95.2700073814 36.3800064721...  POLYGON ((-97.8549229549 38.4194433892, -98.01...


        """
        if distance is None:
            print("buffer"+" cannot be carried on: missing distance")
            return False
        elif not isinstance(distance, Number):
            #mind that distance can be positive or negative
            print("buffer"+" cannot be carried on: distance is not numerical")
            return False
            
        additionalArguments = []
        additionalArguments.append(distance)
        if unit is not None:
            try:
                unit = self._checkLinearUnit(unit, callingMethod="buffer")
            except Exception as e:
                print(e)
                return False
            else:
                additionalArguments.append(unit)
        return self._singleInputFunctionHandler(functionName='ST_Buffer()', 
                                                 columnByUser=colx,
                                                 additionalArguments=additionalArguments, 
                                                 onlyTheseTypes=None)
    
    def centroid(self, colx=None):
        """
        This function takes the geometry column of an IdaGeoDataFrame and computes the geometric center,
        which is the center of the minimum bounding rectangle of the given geometry, as a point (ST_Point).
        The resulting point is represented in the spatial reference system of the given geometry.

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame for which the centroid is to be computed.

        Returns
        -------
        It returns a new column in the IdaGeoDataFrame of type ST_Point which represents the centroid of the geometry
        column of the IdaGeoDataFrame.

        Examples
        --------
        >>> idadf = IdaGeoDataFrame(idadb, 'SAMPLES.GEO_COUNTY', indexer = 'OBJECTID')
        >>> idadf['centroid_of_counties'] = idadf.centroid(colx = 'SHAPE')
       OBJECTID   SHAPE                                              NAME        centroid_of_counties
       538       MULTIPOLYGON (((-84.7169295502 37.8153962541, ...   Garrard     POINT (-84.5466579862 37.6514537302)
       539       MULTIPOLYGON (((-95.4980049329 41.5060813190, ...   Harrison    POINT (-95.8184105459 41.6861673203)
       617       MULTIPOLYGON (((-74.6195926239 40.3744051681, ...   Middlesex   POINT (-74.4165740651 40.4300696882)
       618       MULTIPOLYGON (((-85.5575396424 35.5329837613, ...   Van Buren   POINT (-85.4328451208 35.6775997933)
       619       MULTIPOLYGON (((-93.4905266829 33.0184479233, ...   Columbia    POINT (-93.2345011258 33.2361624817)

        """
        return self._singleInputFunctionHandler(functionName='ST_Centroid()', columnByUser=colx)
        
    def boundary(self, colx=None):
        """
        This function takes the geometry column of an IdaGeoDataFrame and finds its geographic boundary .
        The resulting geometry is represented in the spatial reference system of the given geometry.

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame for which the boundary is to be computed.

        Returns
        -------
        If the given geometry is a point, multipoint, closed curve, or closed multicurve, or if it is empty,
        then the result is an empty geometry of type ST_Point. For curves or multicurves that are not closed,
        the start points and end points of the curves are returned as an ST_MultiPoint value, unless such a
        point is the start or end point of an even number of curves. For surfaces and multisurfaces,
        the curve defining the boundary of the given geometry is returned, either as an ST_Curve or an ST_MultiCurve value.
        If the given geometry is null, then null is returned.

        Examples
        --------
        >>> idadf = IdaGeoDataFrame(idadb, 'SAMPLES.GEO_COUNTY', indexer = 'OBJECTID')
        >>> idadf['boundary_of_counties'] = idadf.boundary(colx = 'SHAPE')
           OBJECTID  NAME               SHAPE                                          boundary_of_counties
           344       Wood        MULTIPOLYGON (((-83.8811535639 41.1678319254, ...     LINESTRING (-83.8811535639 41.1678319254, -83....
           345       Cass        MULTIPOLYGON (((-94.6522670025 33.2688669233, ...     LINESTRING (-94.6522670025 33.2688669233, -94....
           346       Washington  MULTIPOLYGON (((-89.1443938789 38.4738851677, ...     LINESTRING (-89.1443938789 38.4738851677, -89....
           347       Fulton      MULTIPOLYGON (((-74.0974686254 42.9829426852, ...     LINESTRING (-74.0974686254 42.9829426852, -74....
           348       Clay        MULTIPOLYGON (((-96.7646321063 46.9291404309, ...     MULTILINESTRING ((-96.7646321063 46.9291404309.

        """
        return self._singleInputFunctionHandler(functionName='ST_Boundary()', columnByUser=colx)
        
    def envelope(self, colx=None):
        """
        This function takes the geometry column of an IdaGeoDataFrame and finds an envelope
        around the geometry . The envelope is a rectangle that is represented as a polygon.
        The resulting geometry is represented in the spatial reference system of the given geometry
        in a new column of the IdaGeoDataFrame .

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame for which the boundary is to be computed.

        Returns
        -------
        If the given geometry is a point, a horizontal linestring, or a vertical linestring,
        then a rectangle, which is slightly larger than the given geometry, is returned.
        Otherwise, the minimum bounding rectangle of the geometry is returned as the envelope.
        If the given geometry is null or is empty, then null is returned.

        Examples
        --------
        >>> idadf = IdaGeoDataFrame(idadb, 'SAMPLES.GEO_COUNTY', indexer = 'OBJECTID')
        >>> idadf['envelope_of_counties'] = idadf.envelope(colx = 'SHAPE')
            OBJECTID  NAME       SHAPE                                              envelope_of_counties
0           538       Garrard    MULTIPOLYGON (((-84.7169295502 37.8153962541, ...  POLYGON ((-84.7455065514 37.4726702018, -84.34...
1           539       Harrison   MULTIPOLYGON (((-95.4980049329 41.5060813190, ...  POLYGON ((-96.1390201477 41.5060193080, -95.49...
2           617       Middlesex  MULTIPOLYGON (((-74.6195926239 40.3744051681, ...  POLYGON ((-74.6294586256 40.2521281500, -74.20...
3           618       Van Buren  MULTIPOLYGON (((-85.5575396424 35.5329837613, ...  POLYGON ((-85.6121676723 35.5329837613, -85.25...
4           619       Columbia   MULTIPOLYGON (((-93.4905266829 33.0184479233, ...  POLYGON ((-93.4905266829 33.0172739429, -92.97...

        """
        return self._singleInputFunctionHandler(functionName='ST_Envelope()', columnByUser=colx)
    
    def MBR(self, colx=None):
        """
       This function takes the geometry column of an IdaGeoDataFrame and finds the minimum bounding rectangle
       around the geometry .

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame for which the boundary is to be computed.

        Returns
        -------
        If the given geometry is a point, then the point itself is returned. If the geometry is a horizontal linestring
        or a vertical linestring, then the horizontal or vertical linestring itself is returned. Otherwise,
        the minimum bounding rectangle of the geometry is returned as a polygon. If the given geometry is
        null or is empty, then null is returned.

        Examples
        --------
        >>> idadf = IdaGeoDataFrame(idadb, 'SAMPLES.GEO_COUNTY', indexer = 'OBJECTID')
        >>> idadf['envelope_of_counties'] = idadf.MBR(colx = 'SHAPE')
            OBJECTID   DATE             TIME     LEN    WID    SHAPE                                              MBR_of_tornadoes
            115        1950-05-24     13:30:00   3.6    77     MULTILINESTRING ((-99.4800085321 37.2700063950...  POLYGON ((-99.4800085321 37.2700063950, -99.42...
            116        1950-05-24     17:30:00   10.7   40     MULTILINESTRING ((-96.9700080939 39.3800068879...  POLYGON ((-96.9700080939 39.3800068879, -96.78...
            117        1950-05-24     21:30:00   1.0    33     MULTILINESTRING ((-99.0300083486 36.4500062846...  POLYGON ((-99.0300083486 36.4500062846, -99.02...
            118        1950-05-25     16:30:00   3.0    880    MULTILINESTRING ((-101.8800087620 32.670005493...  POLYGON ((-101.8800087620 32.6500054909, -101....
            119        1950-05-29     14:48:00   1.0    33     MULTILINESTRING ((-95.2700073814 36.3800064721...  POLYGON ((-95.2700073814 36.3800064721, -95.26...

        """
        return self._singleInputFunctionHandler(functionName='ST_MBR()', columnByUser=colx)
        
    def SRID(self, colx=None):
        """
       This function takes the geometry column of an IdaGeoDataFrame and finds the ID corresponding to the spatial
        reference system of the geometry .

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame for which the SRID is to be found.

        Returns
        -------
        The current spatial reference system identifier of the given geometry is returned as an integer.
        If srs_id does not identify a spatial reference system listed in the catalog view
        DB2GSE.ST_SPATIAL_REFERENCE_SYSTEMS, then an exception condition is raised (SQLSTATE 38SU1).

        Examples
        --------
        >>> idadf = IdaGeoDataFrame(idadb, 'SAMPLES.GEO_CUSTOMER', indexer = 'OBJECTID')
        >>> idadf['SRID_of geometry'] = idadf.SRID(colx = 'SHAPE')
            OBJECTID    SHAPE                              NAME             INSURANCE_VALUE  SRID_of geometry
            330     POINT (-83.7068206238 32.3855192199)   Wilda Stinger      297057            1005
            331     POINT (-83.5094902518 32.5293427765)   Cassie Mcsweeney   134430            1005
            332     POINT (-83.6036673419 32.3703439716)   Jazmin Castellon   228751            1005
            333     POINT (-83.5113783092 32.4928397102)   Nicky Sprow        555804            1005
            334     POINT (-83.8034685769 32.6024797193)   Louisa Timms       137577            1005

        """
        return self._singleInputFunctionHandler(functionName='ST_SRID()', columnByUser=colx)
        
    def srsName(self, colx=None):
        """
        This function takes the geometry column of an IdaGeoDataFrame and finds the name corresponding to the spatial
        reference system of the geometry from the B2GSE.COORD_REF_SYS catalog view .

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame for which the SRSName is to be found.

        Returns
        -------
        The function returns the name of the spatial reference system in which the given geometry is represented
        as a series of VARCHAR objects.

        Examples
        --------
        >>> idadf = IdaGeoDataFrame(idadb, 'SAMPLES.GEO_CUSTOMER', indexer = 'OBJECTID')
        >>> idadf['srsName'] = idadf.srsName(colx = 'SHAPE')
            OBJECTID 	SHAPE 	                            NAME 	         INSURANCE_VALUE 	 SRS_Name
            1 	      POINT (-80.5561002596 40.1528103049) 	Felice Dicarlo 	     155263 	     SAMPLE_GCS_WGS_1984
            2         POINT (-80.6569863704 40.0406902830)  Aurelia Hussein      201204          SAMPLE_GCS_WGS_1984
            3         POINT (-80.6247752421 40.1320339439)  Hildegard Kittrell   260550          SAMPLE_GCS_WGS_1984
            4         POINT (-80.7158029630 40.1151442910)  Arletta Henne        278992          SAMPLE_GCS_WGS_1984
            5         POINT (-80.6682444120 40.1808573446)  Elvia Shadrick       190152          SAMPLE_GCS_WGS_1984

        """
        return self._singleInputFunctionHandler(functionName='ST_SrsName()', columnByUser=colx)
    
    def geometryType(self, colx=None):
        	"""
        	This function takes the geometry column of an IdaGeoDataFrame and finds the type of the geometry from the predefined geometry types in the DB2 Spatial Extender.

            Parameters
            ----------
            colx : The name of the geometry column in the IdaGeoDataFrame for which the geometry type is to be found.

            Returns
            -------
            The function returns the name of the geometry type like ST_Point, ST_Polygon, ST_Multipolygon, ST_LineString or ST_MultiLineString.

            Examples
            --------
            >>> idadf = IdaGeoDataFrame(idadb, 'SAMPLES.GEO_COUNTY', indexer = 'OBJECTID')
            >>> idadf['srsName'] = idadf.geometryType(colx = 'SHAPE')
                OBJECTID 	NAME 	    SHAPE 	                                            geomType
                 	1 	   Wilbarger 	MULTIPOLYGON (((-99.4756582604 33.8340108094, ... 	"DB2GSE "."ST_MULTIPOLYGON"
 	                2 	   Austin 	    MULTIPOLYGON (((-96.6219873342 30.0442882117, ... 	"DB2GSE "."ST_MULTIPOLYGON"
 	                3 	   Logan 	    MULTIPOLYGON (((-99.4497297204 46.6316377481, ... 	"DB2GSE "."ST_MULTIPOLYGON"
 	                4 	   La Plata 	MULTIPOLYGON (((-107.4817473750 37.0000108736,... 	"DB2GSE "."ST_MULTIPOLYGON"
 	                5 	   Randolph 	MULTIPOLYGON (((-91.2589262966 36.2578866492, ... 	"DB2GSE "."ST_MULTIPOLYGON"

        	"""
        	return self._singleInputFunctionHandler(functionName='ST_GeometryType()', columnByUser=colx)

    def perimeter(self, colx=None, unit=None):
        """
        This function takes the geometry column of an IdaGeoDataFrame and optionally a unit as input parameters and returns the perimeter of the surface or multisurface,
        that is the length of its boundary, measured in the given units.

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame for which the geometry type is to be found.
        unit : The unit in which the perimeter is to be calculated, this is an optional argument.

        Returns
        -------
        The function returns the perimeter of the surface as DOUBLE  or NULL if the given surface or multisurface is null or is empty.

        Examples
        --------
        >>> idadf = IdaGeoDataFrame(idadb, 'SAMPLES.GEO_COUNTY', indexer = 'OBJECTID')
        >>> idadf['perimeter'] = idadf.perimeter(colx = 'SHAPE',unit = ‘KILOMETERS’)
             	OBJECTID 	NAME 	    SHAPE 	                                            perimeter
 	            1 	        Wilbarger 	MULTIPOLYGON (((-99.4756582604 33.8340108094, ... 	247.112571
 	            2 	        Austin 	    MULTIPOLYGON (((-96.6219873342 30.0442882117, ... 	222.841115
 	            3 	        Logan 	    MULTIPOLYGON (((-99.4497297204 46.6316377481, ... 	212.003946
 	            4 	        La Plata 	MULTIPOLYGON (((-107.4817473750 37.0000108736,... 	281.656337
 	            5 	        Randolph 	MULTIPOLYGON (((-91.2589262966 36.2578866492, ... 	195.554056
        """
        additionalArguments = []
        if unit is not None:
            try:
                unit = self._checkLinearUnit(unit, callingMethod="perimeter")
            except Exception as e:
                print(e)
                return False
            else:
                additionalArguments.append(unit)
        return self._singleInputFunctionHandler(functionName='ST_Perimeter()', 
                                         columnByUser=colx,
                                         additionalArguments=additionalArguments, 
                                         onlyTheseTypes=['ST_POLYGON', 'ST_MULTIPOLYGON'])
                                         
    def numGeometries(self, colx=None):
        """
        This function is a wrapper for ST_NumGeometries() . For more information about what the function does, please refer this link :
        http://www.ibm.com/support/knowledgecenter/SSEPGG_9.7.0/com.ibm.db2.luw.spatial.topics.doc/doc/rsbp4074.html

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame.

        """
        return self._singleInputFunctionHandler(functionName='ST_NumGeometries()', 
                                                columnByUser=colx,
                                                onlyTheseTypes=['ST_MULTIPOINT', 'ST_MULTIPOLYGON', 'ST_MULTILINESTRING'])
    def numInteriorRing(self, colx=None):
        """
        This function is a wrapper for ST_NumInteriorRing() . For more information about what the function does, please refer this link :
        http://www.ibm.com/support/knowledgecenter/SSEPGG_9.7.0/com.ibm.db2.luw.spatial.topics.doc/doc/rsbp4098.html

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame.

        """
        return self._singleInputFunctionHandler(functionName='ST_NumInteriorRing()', 
                                                columnByUser=colx,
                                                onlyTheseTypes=['ST_POLYGON'])
                                                 
    def numLineStrings(self, colx=None):
        """
        This function is a wrapper for ST_NumLineStrings() . For more information about what the function does, please refer this link :
        http://www.ibm.com/support/knowledgecenter/SSEPGG_9.7.0/com.ibm.db2.luw.spatial.topics.doc/doc/rsbp4099.html

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame.

        """
        return self._singleInputFunctionHandler(functionName='ST_NumLineStrings()', 
                                                columnByUser=colx,
                                                onlyTheseTypes=['ST_MULTILINESTRING'])

    def numPoints(self, colx=None):
        """
        This function is a wrapper for ST_NumPoints() . For more information about what the function does, please refer this link :
        http://www.ibm.com/support/knowledgecenter/SSEPGG_9.7.0/com.ibm.db2.luw.spatial.topics.doc/doc/rsbp4128.html

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame.

        """
        return self._singleInputFunctionHandler(functionName='ST_NumPoints()', columnByUser=colx)
        
    def numPolygons(self, colx=None):
        """
        This function is a wrapper for ST_NumPolygons() . For more information about what the function does, please refer this link :
        http://www.ibm.com/support/knowledgecenter/SSEPGG_9.7.0/com.ibm.db2.luw.spatial.topics.doc/doc/rsbp4129.html

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame.

        """
        return self._singleInputFunctionHandler(functionName='ST_NumPolygons()', 
                                             columnByUser=colx,
                                             onlyTheseTypes=['ST_MULTIPOLYGON'])

    def coordDim(self, colx=None):
        """
       This function is a wrapper for ST_CoordDim() . For more information about what the function does, please refer this link :
       http://www.ibm.com/support/knowledgecenter/SSEPGG_9.7.0/com.ibm.db2.luw.spatial.topics.doc/doc/rsbp4033.html

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame.

        """
        return self._singleInputFunctionHandler(functionName='ST_CoordDim()', columnByUser=colx)

    def is3d(self, colx=None):
        """
        This function is a wrapper for ST_Is3d() . For more information about what the function does, please refer this link :
        http://www.ibm.com/support/knowledgecenter/SSEPGG_9.7.0/com.ibm.db2.luw.spatial.topics.doc/doc/rsbp4061.html

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame.
        """
        return self._singleInputFunctionHandler(functionName='ST_Is3d()', columnByUser=colx)
        
    def isMeasured(self, colx=None):
        """
        This function is a wrapper for ST_IsMeasured() . For more information about what the function does, please refer this link :
        http://www.ibm.com/support/knowledgecenter/SSEPGG_9.7.0/com.ibm.db2.luw.spatial.topics.doc/doc/rsbp4064.html

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame.
        """
        return self._singleInputFunctionHandler(functionName='ST_IsMeasured()', columnByUser=colx)
        
    def isValid(self, colx=None):
        """
        This function is a wrapper for ST_IsValid() . For more information about what the function does, please refer this link :
        http://www.ibm.com/support/knowledgecenter/SSEPGG_9.7.0/com.ibm.db2.luw.spatial.topics.doc/doc/rsbp4087.html

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame.
        """
        return self._singleInputFunctionHandler(functionName='ST_IsValid()', columnByUser=colx)

    def maxM(self, colx=None):
        """
        This function is a wrapper for ST_MaxM() . For more information about what the function does, please refer this link :
        http://www.ibm.com/support/knowledgecenter/SSEPGG_9.7.0/com.ibm.db2.luw.spatial.topics.doc/doc/rsbp4075.html

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame.

        """
        return self._singleInputFunctionHandler(functionName='ST_MaxM()', columnByUser=colx)
        
    def maxX(self, colx=None):
        """
        This function is a wrapper for ST_MaxX() . For more information about what the function does, please refer this link :
        http://www.ibm.com/support/knowledgecenter/SSEPGG_9.7.0/com.ibm.db2.luw.spatial.topics.doc/doc/csbp4076.html

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame.

        """
        return self._singleInputFunctionHandler(functionName='ST_MaxX()', columnByUser=colx)
        
    def maxY(self, colx=None):
        """
        This function is a wrapper for ST_MaxY() . For more information about what the function does, please refer this link :
        http://www.ibm.com/support/knowledgecenter/SSEPGG_9.7.0/com.ibm.db2.luw.spatial.topics.doc/doc/rsbp4077.html

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame.

        """
        return self._singleInputFunctionHandler(functionName='ST_MaxY()', columnByUser=colx)
        
    def maxZ(self, colx=None):
        """
        This function is a wrapper for ST_MaxY() . For more information about what the function does, please refer this link :
        http://www.ibm.com/support/knowledgecenter/SSEPGG_9.7.0/com.ibm.db2.luw.spatial.topics.doc/doc/rsbp4078.html

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame.

        """
        return self._singleInputFunctionHandler(functionName='ST_MaxZ()', columnByUser=colx)
    
    def minM(self, colx=None):
        """
        This function is a wrapper for ST_MinM() . For more information about what the function does, please refer this link :
        http://www.ibm.com/support/knowledgecenter/SSEPGG_9.7.0/com.ibm.db2.luw.spatial.topics.doc/doc/rsbp4083.html

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame.

        """
        return self._singleInputFunctionHandler(functionName='ST_MinM()', columnByUser=colx)
        
    def minX(self, colx=None):
        """
        This function is a wrapper for ST_MinX() . For more information about what the function does, please refer this link :
        http://www.ibm.com/support/knowledgecenter/SSEPGG_9.7.0/com.ibm.db2.luw.spatial.topics.doc/doc/rsbp4084.html

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame.

        """
        return self._singleInputFunctionHandler(functionName='ST_MinX()', columnByUser=colx)
    
    def minY(self, colx=None):
        """
        This function is a wrapper for ST_MinY() . For more information about what the function does, please refer this link :
        http://www.ibm.com/support/knowledgecenter/SSEPGG_9.7.0/com.ibm.db2.luw.spatial.topics.doc/doc/rsbp4085.html

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame.

        """
        return self._singleInputFunctionHandler(functionName='ST_MinY()', columnByUser=colx)
        
    def minZ(self, colx=None):
        """
        This function is a wrapper for ST_MinZ() . For more information about what the function does, please refer this link :
        http://www.ibm.com/support/knowledgecenter/SSEPGG_9.7.0/com.ibm.db2.luw.spatial.topics.doc/doc/rsbp4086.html

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame.

        """
        return self._singleInputFunctionHandler(functionName='ST_MinM()', columnByUser=colx)
        
    def M(self, colx=None):
        """
        This function is a wrapper for ST_MinZ() . For more information about what the function does, please refer this link :
       http://www.ibm.com/support/knowledgecenter/SSEPGG_9.7.0/com.ibm.db2.luw.spatial.topics.doc/doc/rsbp4093.html

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame.

        """
        return self._singleInputFunctionHandler(functionName='ST_M()', 
                                         columnByUser=colx, 
                                         onlyTheseTypes=['ST_POINT'])
                                         
    def X(self, colx=None):
        """
        This function takes the geometry column of an IdaGeoDataFrame of type ST_POINT and  returns the X coordinate of the point,
        that is the length of its boundary, measured in the given units.

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame for which the geometry type is to be found.
        unit : The unit in which the perimeter is to be calculated, this is an optional argument.

        Returns
        -------
        The function returns the perimeter of the surface as DOUBLE  or NULL if the given surface or multisurface is null or is empty.


        Examples
        --------
        >>> idadf = IdaGeoDataFrame(idadb, 'SAMPLES.GEO_CUSTOMER', indexer = 'OBJECTID')
        >>> idadf['perimeter'] = idadf.X(colx = 'SHAPE')
         	OBJECTID 	SHAPE 	                                NAME 	            INSURANCE_VALUE 	X
 	        1 	        POINT (-80.5561002596 40.1528103049) 	Felice Dicarlo 	    155263 	           -80.556100
 	        2 	        POINT (-80.6569863704 40.0406902830) 	Aurelia Hussein 	201204 	           -80.656986
 	        3 	        POINT (-80.6247752421 40.1320339439) 	Hildegard Kittrell 	260550 	           -80.624775
 	        4 	        POINT (-80.7158029630 40.1151442910) 	Arletta Henne 	    278992 	           -80.715803
 	        5 	        POINT (-80.6682444120 40.1808573446) 	Elvia Shadrick 	    190152 	           -80.668244

        """
        return self._singleInputFunctionHandler(functionName='ST_X()', 
                                         columnByUser=colx, 
                                         onlyTheseTypes=['ST_POINT'])
                                         
    def Y(self, colx=None):
        """
       This function takes the geometry column of an IdaGeoDataFrame of type ST_POINT and returns the Y coordinate of the point.

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame for which the geometry type is to be found.
        unit : The unit in which the perimeter is to be calculated, this is an optional argument.

        Returns
        -------
        The function returns the perimeter of the surface as DOUBLE  or NULL if the given surface or multisurface is null or is empty.


        Examples
        --------
        >>> idadf = IdaGeoDataFrame(idadb, 'SAMPLES.GEO_CUSTOMER', indexer = 'OBJECTID')
        >>> idadf['perimeter'] = idadf.X(colx = 'SHAPE')
         	OBJECTID 	SHAPE 	                                NAME 	            INSURANCE_VALUE 	Y
 	        1 	        POINT (-80.5561002596 40.1528103049) 	Felice Dicarlo 	    155263 	           40.152810
 	        2 	        POINT (-80.6569863704 40.0406902830) 	Aurelia Hussein 	201204 	           40.040690
 	        3 	        POINT (-80.6247752421 40.1320339439) 	Hildegard Kittrell 	260550 	           40.132034
 	        4 	        POINT (-80.7158029630 40.1151442910) 	Arletta Henne 	    278992 	           40.115144
 	        5 	        POINT (-80.6682444120 40.1808573446) 	Elvia Shadrick 	    190152 	           40.180857

        """
        return self._singleInputFunctionHandler(functionName='ST_Y()', 
                                         columnByUser=colx, 
                                         onlyTheseTypes=['ST_POINT'])
                                         
    def Z(self, colx=None):
        """
        This function takes the geometry column of an IdaGeoDataFrame of type ST_POINT and returns the Z coordinate of the point.

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame for which the geometry type is to be found.
        unit : The unit in which the perimeter is to be calculated, this is an optional argument.

        Returns
        -------
        The function returns the perimeter of the surface as DOUBLE  or NULL/None if the given surface or multisurface is null or is empty.


        Examples
        --------
        >>> idadf = IdaGeoDataFrame(idadb, 'SAMPLES.GEO_CUSTOMER', indexer = 'OBJECTID')
        >>> idadf['perimeter'] = idadf.X(colx = 'SHAPE')
         	OBJECTID 	SHAPE 	                                NAME 	            INSURANCE_VALUE 	Z
 	        1 	        POINT (-80.5561002596 40.1528103049) 	Felice Dicarlo 	    155263 	           None
 	        2 	        POINT (-80.6569863704 40.0406902830) 	Aurelia Hussein 	201204 	           None
 	        3 	        POINT (-80.6247752421 40.1320339439) 	Hildegard Kittrell 	260550 	           None
 	        4 	        POINT (-80.7158029630 40.1151442910) 	Arletta Henne 	    278992 	           None
 	        5 	        POINT (-80.6682444120 40.1808573446) 	Elvia Shadrick 	    190152 	           None
        """
        return self._singleInputFunctionHandler(functionName='ST_Z()', 
                                         columnByUser=colx, 
                                         onlyTheseTypes=['ST_POINT'])
    def isClosed(self, colx=None):
        """
        This function is a wrapper for ST_isClosed() . For more information about what the function does, please refer this link :
        http://www.ibm.com/support/knowledgecenter/SSEPGG_9.7.0/com.ibm.db2.luw.spatial.topics.doc/doc/rsbp4062.html

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame.
        """
        return self._singleInputFunctionHandler(functionName='ST_IsClosed()', 
                                                columnByUser=colx, 
                                                onlyTheseTypes=['ST_LINESTRING', 'ST_MULTILINESTRING'])                                   
    
    def isEmpty(self, colx=None):
        """
        This function is a wrapper for ST_isEmpty() . For more information about what the function does, please refer this link :
        http://www.ibm.com/support/knowledgecenter/SSEPGG_9.7.0/com.ibm.db2.luw.spatial.topics.doc/doc/rsbp4063.html

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame.
        """
        return self._singleInputFunctionHandler(functionName='ST_IsEmpty()', columnByUser=colx)
    
    def isSimple(self, colx=None):
        """
        This function is a wrapper for ST_isSimple() . For more information about what the function does, please refer this link :
        http://www.ibm.com/support/knowledgecenter/SSEPGG_9.7.0/com.ibm.db2.luw.spatial.topics.doc/doc/rsbp4067.html

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame.
        """
        return self._singleInputFunctionHandler(functionName='ST_IsSimple()', columnByUser=colx)  

    def area(self, colx=None, unit=None):
        """
        This function takes the geometry column of an IdaGeoDataFrame and optionally, a unit as input parameters and
        returns the area covered by the given geometry in the given unit of measure .

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame for which the area is to be found.

        Returns
        -------
        The function returns the the area covered by the geometry if the geometry is a polygon or multipolygon.
        The area of points, linestrings, multipoints, and multilinestrings is 0 (zero).
        If the geometry is null or is an empty geometry, then null is returned.

        Examples
        --------
        >>> idadf = IdaGeoDataFrame(idadb, 'SAMPLES.GEO_COUNTY', indexer = 'OBJECTID')
        >>> idadf['area'] = idadf.area(colx = 'SHAPE', unit = 'STATUTE MILE')
         	OBJECTID 	NAME 	        SHAPE 	                                                 area
        0 	1 	        Wilbarger 	MULTIPOLYGON (((-99.4756582604 33.8340108094, ... 	0.247254
        1 	2 	        Austin 	        MULTIPOLYGON (((-96.6219873342 30.0442882117, ... 	0.162639
        2 	3 	        Logan 	        MULTIPOLYGON (((-99.4497297204 46.6316377481, ... 	0.306589
        3 	4 	        La Plata 	MULTIPOLYGON (((-107.4817473750 37.0000108736,... 	0.447591
        4 	5 	        Randolph 	MULTIPOLYGON (((-91.2589262966 36.2578866492, ... 	0.170844
        """
        additionalArguments = []
        if unit is not None:
            try:
                unit = self._checkLinearUnit(unit, callingMethod="area")
            except Exception as e:
                print(e)
                return False
            else:
                additionalArguments.append(unit)
        return self._singleInputFunctionHandler(functionName='ST_Area()', 
                                                columnByUser=colx,
                                                additionalArguments=additionalArguments)

        
    def dimension(self, colx=None):
        """
        This function takes the geometry column of an IdaGeoDataFrame and
        returns the dimension of the geometry .

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame for which the dimension is to be found.

        Returns
        -------
        If the given geometry is empty, then -1 is returned. For points and
        multipoints, the dimension is 0 (zero); for curves and multicurves, the
        dimension is 1; and for polygons and multipolygons, the dimension is 2. If the
        given geometry is null, then null is returned0 (zero).
        If the geometry is null or is an empty geometry, then null is returned.

        Examples
        --------
        >>> idadf = IdaGeoDataFrame(idadb, 'SAMPLES.GEO_COUNTY', indexer = 'OBJECTID')
        >>> idadf['dim'] = idadf.dimension(colx = 'SHAPE')
         	OBJECTID 	NAME 	        SHAPE 	                                               dim
        0 	1 	        Wilbarger 	MULTIPOLYGON (((-99.4756582604 33.8340108094, ... 	2
        1 	2 	        Austin 	        MULTIPOLYGON (((-96.6219873342 30.0442882117, ... 	2
        2 	3 	        Logan 	        MULTIPOLYGON (((-99.4497297204 46.6316377481, ... 	2
        3 	4 	        La Plata 	MULTIPOLYGON (((-107.4817473750 37.0000108736,... 	2
        4 	5 	        Randolph 	MULTIPOLYGON (((-91.2589262966 36.2578866492, ... 	2

        >>> idadf = IdaGeoDataFrame(idadb, 'SAMPLES.GEO_CUSTOMER', indexer = 'OBJECTID')
        >>> idadf['dim'] = idadf.dimension(colx = 'SHAPE')
         	OBJECTID 	SHAPE 	                                NAME 	            INSURANCE_VALUE 	dim
        0 	1 	        POINT (-80.5561002596 40.1528103049) 	Felice Dicarlo 	        155263 	        0
        1 	2 	        POINT (-80.6569863704 40.0406902830) 	Aurelia Hussein 	201204 	        0
        2 	3 	        POINT (-80.6247752421 40.1320339439) 	Hildegard Kittrell 	260550 	        0
        3 	4 	        POINT (-80.7158029630 40.1151442910) 	Arletta Henne 	        278992 	        0
        4 	5 	        POINT (-80.6682444120 40.1808573446) 	Elvia Shadrick 	        190152 	        0

        >>> idadf = IdaGeoDataFrame(idadb, 'SAMPLES.GEO_TORNADO', indexer = 'OBJECTID')
        >>> idadf['dim'] = idadf.dimension(colx = 'SHAPE')
         	OBJECTID 	SHAPE 	                                            dim
        0 	1 	        MULTILINESTRING ((-90.2200062071 38.7700071663... 	1
        1 	2 	        MULTILINESTRING ((-89.3000059755 39.1000072739... 	1
        2 	3 	        MULTILINESTRING ((-84.5800047496 40.8800078382... 	1
        3 	4 	        MULTILINESTRING ((-94.3700070010 34.4000061520... 	1
        4 	5 	        MULTILINESTRING ((-90.6800062393 37.6000069289... 	1
        """
        return self._singleInputFunctionHandler(functionName='ST_Dimension()', columnByUser=colx)
        
    def length(self, colx=None, unit=None):
        """
        This function takes the geometry column of an IdaGeoDataFrame of type ST_Linestring or ST_MultiLineString
        and optionally, a unit as input parameters and returns the length of the given geometry in the
        given unit of measure .

        Parameters
        ----------
        colx : The name of the geometry column in the IdaGeoDataFrame for which the length is to be found.

        Returns
        -------
        The function returns the length of the given curve or multicurve in the given unit of measure.
        If the given curve or multicurve is null or is empty, then null is returned..

        Examples
        --------
        >>> idadf = IdaGeoDataFrame(idadb, 'SAMPLES.GEO_TORNADO', indexer = 'OBJECTID')
        >>> idadf['length'] = idadf.area(colx = 'SHAPE', unit = 'KILOMETRES')
         	OBJECTID 	SHAPE 	                                         length
        0 	115 	    MULTILINESTRING ((-99.4800085321 37.2700063950... 	5.435906
        1 	116 	    MULTILINESTRING ((-96.9700080939 39.3800068879... 	18.114294
        2 	117 	    MULTILINESTRING ((-99.0300083486 36.4500062846... 	0.014205
        3 	118 	    MULTILINESTRING ((-101.8800087620 32.670005493... 	3.583249
        4 	119 	    MULTILINESTRING ((-95.2700073814 36.3800064721... 	0.014204
        """
        additionalArguments = []
        if unit is not None:
            try:
                unit = self._checkLinearUnit(unit, callingMethod="length")
            except Exception as e:
                print(e)
                return False
            else:
                additionalArguments.append(unit)
        return self._singleInputFunctionHandler(functionName='ST_Length()', 
                                                columnByUser=colx,
                                                additionalArguments=additionalArguments,
                                                onlyTheseTypes=['ST_LINESTRING', 'ST_MULTILINESTRING'])
        
    


#==============================================================================
### Public utilities for geospatial methods
#==============================================================================
            
    @lazy
    def linearUnits(self):
        units = self.ida_query('SELECT UNIT_NAME FROM DB2GSE.ST_UNITS_OF_MEASURE WHERE '
        'UNIT_TYPE= \'LINEAR\' ORDER BY LENGTH(UNIT_NAME), UNIT_NAME')        
        return units
    
    def setGeometry(self, columnName=None):
        """
        Sets the given column name as _geometry attribute if the column is
        present in the IdaGeoDataFrame and it has geometry type.
        If columnName is None, then geometry attribute is set to None.
        
        Parameters
        ----------
        columnName : str
            The column name to be set as the IdaGeoDataFrame's default 
            geometry column
            
        Returns
        -------
        bool
            True if successful
            
        Raises
        ------
        IdaGeoDataFrameError
            If the column is not present in the IdaGeoDataFrame
        IdaGeoDataFrameError
            If the given column doesn't have geometry type           
        """
        
        if columnName is None:
            self._geometry = None
            return True
        if columnName not in self.columns:
            raise IdaGeoDataFrameError("'"+columnName+"' cannot be used as "+
            "default geometry column because this is not a column in "+
            self._name)
            print("Hint:\n"+
                "dtypes attribute shows the column names and their types\n")
        elif columnName not in self._dtypesGeometrical().index:
            raise IdaGeoDataFrameError("'"+columnName+"' cannot be used as "+
            "default geometry column because it doesn't have geometry type")
            print("Hint:\n"+
            "dtypes attribute shows the column names and their types\n")
        else:
            #columnName is in the IdaGeoDataFrame and has geometry type
            self._geometry = columnName
            return True
        
#==============================================================================
### Private utilities for geospatial methods
#==============================================================================

    def _dtypesGeometrical(idadf):
        """
        Returns a dataframe with the following info of the geometry columns in 
        the IdaDataFrame:
        COLNAME(Index), TYPENAME, COLNUMBER(0-based)    
        """
        table = []
        dtypes = idadf.dtypes
        for colNumber, colNameAndType in enumerate(zip(dtypes['TYPENAME'].index,
                                      dtypes['TYPENAME'])):
            colName = colNameAndType[0]
            colType = colNameAndType[1]
            if(colType.find('ST_') == 0): #ST_ at the begining of the string
                table.append([colName, colType, colNumber])
        columns = ['COLNAME', 'TYPENAME', 'COLNUMBER']
        result = pd.DataFrame(table, columns=columns)    
        result.set_index(keys='COLNAME', inplace=True)
        return result
    
    def _checkLinearUnit(self, unit, callingMethod):
        """
        Parameters:
        -----------
        unit : str
            name of a user-entered unit
        callingMethod : str
            The name of the calling method
        
        Returns
        -------
        str
            The name of the unit in uppercase and formatted for DB2GSE syntax
            if it is a valid linear unit
            
        Raises
        ------
        IdaGeoDataFrameError
            If the given unit is not a valid linear unit
        """

        if not isinstance(unit, six.string_types):
            raise TypeError(callingMethod+" cannot be carried on: the unit must be a string")
        else:
            unit = unit.upper()
            if unit not in self.linearUnits.tolist():
                raise IdaGeoDataFrameError(callingMethod+" cannot be carried "+
                "on: invalid unit\n Hint: use linearUnits attribute to get a "+
                "list of valid units")
            else:
                #replace single quotation marks with two of them
                if "\'" in unit:
                    unit = unit.replace("'", "''")  

                #enclose in single quotation marks
                unit = '\''+unit+'\''
                return unit
        
    def _getColumnForSingleInputFunction(self, columnByUser, functionName,
                                                  onlyTheseTypes):
        """
        Gets a valid column and returns its definition. Raises the proper 
        exceptions as needed.
        
        The columnByUser column has precedence over the default geometry 
        column of the IdaGeoDataFrame (_geometry attribute)
        
        Returns
        -------
        str
            column definition if successful
        
        Raises
        ------
        IdaGeoDataFrameError
            * If columnByUser is None and default geometry is not set
            * If columnByUser is None and default geometry has incompatible
              type for function
            * If columnByUser is not in the IdaGeoDataFrame
            * If columnByUser has incompatible type for the function
        """
        columnName = ''
        if columnByUser is None:
            #user didn't specify a column name when calling the geospatial method
            if self._geometry is None:
                #the IdaGeoDataFrame doesn't have a (default) geometry column
                #return False
                raise IdaGeoDataFrameError(functionName+" cannot be carried "+
                "on: no column was specified and default geometry column is "+
                "not set")
            else:
                #the IdaGeoDataFrame has a default geometry column
                if( (onlyTheseTypes is None) or 
                (self.dtypes.TYPENAME[self._geometry] in onlyTheseTypes)):
                    #no type restriction or column has compatible types
                    columnName = self._geometry
                else:
                    #the default geometry column has incompatible types for the
                    #DB2GSE function
                    #return False
                    raise IdaGeoDataFrameError(functionName+" cannot be "+
                    "carried on: no column was specified and the default "
                    "geometry column '"+self._geometry+"' has incompatible type")
        else:
            #user specified a column name
            if columnByUser not in self.columns:
                #column not in the IdaGeoDataFrame
                raise IdaGeoDataFrameError(functionName+" cannot be carried "+
                "on '"+columnByUser+"': column not in "+self._name)
            else:
                #column in the IdaGeoDataFrame
                if( (onlyTheseTypes is None) or 
                (self.dtypes.TYPENAME[columnByUser] in onlyTheseTypes)):
                    #no type restriction or column has compatible types
                    columnName = columnByUser
                else:
                    #incompatible type
                    raise IdaGeoDataFrameError(functionName+" cannot be "
                    "carried on '"+columnByUser+"': column has incompatible "+
                    "type")
        
        columnDefinition = self.internal_state.columndict[columnName]
        if columnDefinition[0] == '\"' and columnDefinition[-1] == '\"':
            columnDefinition = columnDefinition[1:-1]
        return columnDefinition
    
    def _setGeometryAutomatically(self, verbose = True):
        """
        Sets the _geometry attribute of the IdaGeoDataFrame automatically.
        If the IdaGeoDataFrame has more than one geometry column, the
        left-most one is used
        If the IdaGeoDataFrame doesn't have geometry columns, the attribute
        is set to None
        """
        geoCols = self._dtypesGeometrical()
        if len(geoCols) == 0:
            self.setGeometry(columnName = None)
            if verbose:
                print("There are no geometry columns in the IdaGeoDataFrame\n"+
                "No default geometry for geospatial operations\n")
        elif len(geoCols) == 1:
            self.setGeometry(columnName = geoCols.index[0])
            if verbose:
                print("Column '"+self._geometry+"' default "+
                "geometry column for geospatial operations.\n")
        elif len(geoCols) > 1:
            self.setGeometry(columnName = geoCols.index[0])
            if verbose:
                print("Column '"+self._geometry+"' default "+
                "geometry column for geospatial operations.\n")
    
    def _singleInputFunctionHandler(self, functionName=None, 
                                    columnByUser=None,
                                    additionalArguments=None, 
                                    onlyTheseTypes=None):
        """
        Returns a one-column IdaGeoDataFrame with the specified DB2GSE function 
        as column, carried on the given column.
        
        The returned IdaGeoDataFrame is generated by cloning the self 
        IdaGeoDataFrame, adding the proper column to the columndict and 
        then use __getitem__ to return only the just-added column.
        
        The name and definition of the only column in the returned
        IdaGeoDataFrame is the same, namely, the column name with
        DB2GSE SQL syntax e.g. "ST_Buffer(SHAPE,2.3,'KILOMETER')"
        
        The _geometry attribute of the returned IdaGeoDataFrame is set to the
        only column it has

        The intuition behind cloning the original IdaGeoDataFrame is to preserve
        attributes like indexer, etc.
        
        Parameters:
        -----------
        
        functionName : str
            DB2GSE function name
        columnByUser : str
            column name specified by the user
        additionalArguments : list
            List of already validated arguments for the DB2GSE function.
        onlyTheseTypes : list
            List of the only valid datatypes for the given DB2GSE function
                    
        """
                
        newGeoidadf = self._clone()
        try:
            workingColumn = self._getColumnForSingleInputFunction(columnByUser,
                                                                functionName,
                                                                onlyTheseTypes)                                                         
        except Exception as e:
            print(e)
        else:
            #define the column that is going to be added to the definition of 
            #the IdaDataFrame
            argumentsForSTFunction = []
            argumentsForSTFunction.append(workingColumn)

            if additionalArguments is not None:
                for argument in additionalArguments:
                    argumentsForSTFunction.append(argument)
                    
            newColumn = functionName.replace('()', '('+
            ','.join(map(str,argumentsForSTFunction))+')')           
            
            #form the new columndict
            newGeoidadf.internal_state.columndict[newColumn] = newColumn
            newColumndict = newGeoidadf.internal_state.columndict
            #erase attributes
            newGeoidadf._reset_attributes(["columns", "shape", "dtypes"])
            #set columns and columndict attributes
            newGeoidadf.internal_state.columns = ["\"%s\""%col for col in newColumndict.keys()]
            newGeoidadf.internal_state.columndict = newColumndict
            #update, i.e. appends an entry to internal_state._cumulative
            newGeoidadf.internal_state.update()
            
            #get only the new column
            columnIda = newGeoidadf[newColumn]
            #set the _geometry attribute
            columnIda._geometry = newColumn
            return columnIda
            
    def _doubleInputFunctionHandler(self):
        pass
    