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
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_Buffer
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
        DB2GSE category: Construction of new geometries from existing geometries 
        DB2GSE function: ST_Centroid
        """
        return self._singleInputFunctionHandler(functionName='ST_Centroid()', columnByUser=colx)
        
    def boundary(self, colx=None):
        """
        DB2GSE category: Construction of new geometries from existing geometries 
        DB2GSE function: ST_Boundary
        """
        return self._singleInputFunctionHandler(functionName='ST_Boundary()', columnByUser=colx)
        
    def envelope(self, colx=None):
        """
        DB2GSE category: Construction of new geometries from existing geometries 
        DB2GSE function: ST_Envelope
        """
        return self._singleInputFunctionHandler(functionName='ST_Envelope()', columnByUser=colx)
    
    def MBR(self, colx=None):
        """
        DB2GSE category: Construction of new geometries from existing geometries 
        DB2GSE function: ST_MBR
        """
        return self._singleInputFunctionHandler(functionName='ST_MBR()', columnByUser=colx)
        
    def SRID(self, colx=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_SRID
        """
        return self._singleInputFunctionHandler(functionName='ST_SRID()', columnByUser=colx)
        
    def srsName(self, colx=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_SrsName
        """
        return self._singleInputFunctionHandler(functionName='ST_SrsName()', columnByUser=colx)
    
    def geometryType(self, colx=None):
        	"""
        	DB2GSE category: Information about spatial indexes and geometries 
        	DB2GSE function: ST_GeometryType
        	"""
        	return self._singleInputFunctionHandler(functionName='ST_GeometryType()', columnByUser=colx)

    def perimeter(self, colx=None, unit=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_Perimeter
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
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_NumGeometries
        """
        return self._singleInputFunctionHandler(functionName='ST_NumGeometries()', 
                                                columnByUser=colx,
                                                onlyTheseTypes=['ST_MULTIPOINT', 'ST_MULTIPOLYGON', 'ST_MULTILINESTRING'])
    def numInteriorRing(self, colx=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_NumInteriorRing
        """
        return self._singleInputFunctionHandler(functionName='ST_NumInteriorRing()', 
                                                columnByUser=colx,
                                                onlyTheseTypes=['ST_POLYGON'])
                                                 
    def numLineStrings(self, colx=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_NumLineStrings
        """
        return self._singleInputFunctionHandler(functionName='ST_NumLineStrings()', 
                                                columnByUser=colx,
                                                onlyTheseTypes=['ST_MULTILINESTRING'])

    def numPoints(self, colx=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_NumPoints
        """
        return self._singleInputFunctionHandler(functionName='ST_NumPoints()', columnByUser=colx)
        
    def numPolygons(self, colx=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_NumPolygons
        """
        return self._singleInputFunctionHandler(functionName='ST_NumPolygons()', 
                                             columnByUser=colx,
                                             onlyTheseTypes=['ST_MULTIPOLYGON'])

    def coordDim(self, colx=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_CoordDim
        """
        return self._singleInputFunctionHandler(functionName='ST_CoordDim()', columnByUser=colx)

    def is3d(self, colx=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_Is3d
        """
        return self._singleInputFunctionHandler(functionName='ST_Is3d()', columnByUser=colx)
        
    def isMeasured(self, colx=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_IsMeasured
        """
        return self._singleInputFunctionHandler(functionName='ST_IsMeasured()', columnByUser=colx)
        
    def isValid(self, colx=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_IsValid
        """
        return self._singleInputFunctionHandler(functionName='ST_IsValid()', columnByUser=colx)

    def maxM(self, colx=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_MaxM
        """
        return self._singleInputFunctionHandler(functionName='ST_MaxM()', columnByUser=colx)
        
    def maxX(self, colx=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_MaX
        """
        return self._singleInputFunctionHandler(functionName='ST_MaxX()', columnByUser=colx)
        
    def maxY(self, colx=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_MaxY
        """
        return self._singleInputFunctionHandler(functionName='ST_MaxY()', columnByUser=colx)
        
    def maxZ(self, colx=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_MaxZ
        """
        return self._singleInputFunctionHandler(functionName='ST_MaxZ()', columnByUser=colx)
    
    def minM(self, colx=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_MinM
        """
        return self._singleInputFunctionHandler(functionName='ST_MinM()', columnByUser=colx)
        
    def minX(self, colx=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_MinX
        """
        return self._singleInputFunctionHandler(functionName='ST_MinX()', columnByUser=colx)
    
    def minY(self, colx=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_MinY
        """
        return self._singleInputFunctionHandler(functionName='ST_MinY()', columnByUser=colx)
        
    def minZ(self, colx=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_MinM
        """
        return self._singleInputFunctionHandler(functionName='ST_MinM()', columnByUser=colx)
        
    def M(self, colx=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_M
        """
        return self._singleInputFunctionHandler(functionName='ST_M()', 
                                         columnByUser=colx, 
                                         onlyTheseTypes=['ST_POINT'])
                                         
    def X(self, colx=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_X
        """
        return self._singleInputFunctionHandler(functionName='ST_X()', 
                                         columnByUser=colx, 
                                         onlyTheseTypes=['ST_POINT'])
                                         
    def Y(self, colx=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_Y
        """
        return self._singleInputFunctionHandler(functionName='ST_Y()', 
                                         columnByUser=colx, 
                                         onlyTheseTypes=['ST_POINT'])
                                         
    def Z(self, colx=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_Z
        """
        return self._singleInputFunctionHandler(functionName='ST_Z()', 
                                         columnByUser=colx, 
                                         onlyTheseTypes=['ST_POINT'])
    def isClosed(self, colx=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_IsClosed
        """
        return self._singleInputFunctionHandler(functionName='ST_IsClosed()', 
                                                columnByUser=colx, 
                                                onlyTheseTypes=['ST_LINESTRING', 'ST_MULTILINESTRING'])                                   
    
    def isEmpty(self, colx=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_IsEmpty
        """
        return self._singleInputFunctionHandler(functionName='ST_IsEmpty()', columnByUser=colx)
    
    def isSimple(self, colx=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_IsSimple
        """
        return self._singleInputFunctionHandler(functionName='ST_IsSimple()', columnByUser=colx)  

    def area(self, colx=None, unit=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_Area
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
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_Dimension
        """
        return self._singleInputFunctionHandler(functionName='ST_Dimension()', columnByUser=colx)
        
    def length(self, colx=None, unit=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_Length
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
    