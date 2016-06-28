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
    methods for calling functions of DB2/DashDB Geospatial Extender (DB2GSE).
    See IdaDataFrame.

    Examples
    --------
    >>> idadf = IdaGeoDataFrame(idadb, 'SAMPLES.GEO_TORNADO', indexer = 'OBJECTID')
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
        
        Notes
        -----
        setGeometry() can raise Exceptions
        """
        super(IdaGeoDataFrame, self).__init__(idadb, tablename, indexer)
        if geometry is not None:
            geometry = geometry.upper()
            #set the _geometry attribute
            self.setGeometry(geometry) #will raise an exception as appropriate
        else:
            self._setGeometryAutomatically(verbose = verboseGeometryColumn)           
    
    @classmethod
    def fromIdaDataFrame(self, idadf = None, geometry = None):
        """
        "Alternate" constructor. 
        Create an IdaGeoDataFrame from an IdaDataFrame (or an IdaGeoDataFrame)
        
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
            
        Raises
        ------
        TypeError
            If idadf is not an IdaDataFrame
            
        Notes
        -----
        setGeometry() can raise Exceptions            
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
        Overriden method from IdaDataFrame. Differences:
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
                return self.loc[item]
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
            Overriden method from IdaDataFrame. Difference: 
            Returns an IdaGeoDataFrame instead of an IdaDataFrame
            
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
        DB2GSE category: Construction of new geometries from existing geometries 
        DB2GSE function: ST_Buffer
        """
        if distance is None:
            raise TypeError("Missing distance")
        elif not isinstance(distance, Number):
            #mind that distance can be positive or negative
            raise TypeError("Distance is not numerical")
            
        additionalArguments = []
        additionalArguments.append(distance)
        if unit is not None:
            unit = self._checkLinearUnit(unit)
            additionalArguments.append(unit)
        return self._singleInputFunctionHandler(functionName='ST_Buffer()', 
                                                 columnByUser=colx,
                                                 additionalArguments=additionalArguments)
    
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

    def area(self, colx=None, unit=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_Area
        """
        additionalArguments = []
        if unit is not None:
            unit = self._checkLinearUnit(unit)
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
            unit = self._checkLinearUnit(unit)
            additionalArguments.append(unit)
        return self._singleInputFunctionHandler(functionName='ST_Length()', 
                                                columnByUser=colx,
                                                additionalArguments=additionalArguments,
                                                validInputTypes=['ST_LINESTRING', 'ST_MULTILINESTRING'])
        
    def perimeter(self, colx=None, unit=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_Perimeter
        """
        additionalArguments = []
        if unit is not None:
            unit = self._checkLinearUnit(unit)
            additionalArguments.append(unit)
        return self._singleInputFunctionHandler(functionName='ST_Perimeter()', 
                                         columnByUser=colx,
                                         additionalArguments=additionalArguments, 
                                         validInputTypes=['ST_POLYGON', 'ST_MULTIPOLYGON'])
                                         
    def numGeometries(self, colx=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_NumGeometries
        """
        return self._singleInputFunctionHandler(functionName='ST_NumGeometries()', 
                                                columnByUser=colx,
                                                validInputTypes=['ST_MULTIPOINT', 'ST_MULTIPOLYGON', 'ST_MULTILINESTRING'])
    def numInteriorRing(self, colx=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_NumInteriorRing
        """
        return self._singleInputFunctionHandler(functionName='ST_NumInteriorRing()', 
                                                columnByUser=colx,
                                                validInputTypes=['ST_POLYGON'])
                                                 
    def numLineStrings(self, colx=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_NumLineStrings
        """
        return self._singleInputFunctionHandler(functionName='ST_NumLineStrings()', 
                                                columnByUser=colx,
                                                validInputTypes=['ST_MULTILINESTRING'])

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
                                             validInputTypes=['ST_MULTIPOLYGON'])

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
                                         validInputTypes=['ST_POINT'])
                                         
    def X(self, colx=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_X
        """
        return self._singleInputFunctionHandler(functionName='ST_X()', 
                                         columnByUser=colx, 
                                         validInputTypes=['ST_POINT'])
                                         
    def Y(self, colx=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_Y
        """
        return self._singleInputFunctionHandler(functionName='ST_Y()', 
                                         columnByUser=colx, 
                                         validInputTypes=['ST_POINT'])
                                         
    def Z(self, colx=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_Z
        """
        return self._singleInputFunctionHandler(functionName='ST_Z()', 
                                         columnByUser=colx, 
                                         validInputTypes=['ST_POINT'])
    def isClosed(self, colx=None):
        """
        DB2GSE category: Information about spatial indexes and geometries 
        DB2GSE function: ST_IsClosed
        """
        return self._singleInputFunctionHandler(functionName='ST_IsClosed()', 
                                                columnByUser=colx, 
                                                validInputTypes=['ST_LINESTRING', 'ST_MULTILINESTRING'])                                   
    
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
            
        Raises
        ------
        KeyError
            If the column is not present in the IdaGeoDataFrame
        IdaGeoDataFrameError
            If the given column doesn't have geometry type           
        """
        
        if columnName is None:
            self._geometry = None
        elif columnName not in self.columns:
            raise KeyError("'"+columnName+"' cannot be set as "+
            "default geometry column because this is not a column in "+
            self._name)
            print("Hint:\n"+
                "dtypes attribute shows the column names and their types\n")
        elif columnName not in self._dtypesGeometrical().index:
            raise IdaGeoDataFrameError("'"+columnName+"' cannot be set as "+
            "default geometry column because it doesn't have geometry type")
            print("Hint:\n"+
            "dtypes attribute shows the column names and their types\n")
        else:
            #columnName is in the IdaGeoDataFrame and has geometry type
            self._geometry = columnName
        
#==============================================================================
### Private utilities for geospatial methods
#==============================================================================

    def _dtypesGeometrical(idadf):
        """
        Returns a DataFrame with the following info of the geometry columns in 
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
    
    def _checkLinearUnit(self, unit):
        """
        Parameters:
        -----------
        unit : str
            Name of a case-insensitive user-entered unit
        
        Returns
        -------
        str
            The name of the unit in uppercase and formatted for DB2GSE syntax
            if it is a valid linear unit
            
        Raises
        ------
        TypeError
            * If the unit is not a string
            * If the unit is a string larger than VARCHAR(128)
        IdaGeoDataFrameError
            If the given unit is not a valid linear unit of DB2GSE
        """

        if not isinstance(unit, six.string_types):
            raise TypeError("Unit must be a string")
        elif len(unit) > 128:
            raise TypeError("Unit length exceeded" )
        else:
            unit = unit.upper()
            if unit not in self.linearUnits.tolist():
                raise IdaGeoDataFrameError("Invalid unit\n"
                " Hint: use linearUnits attribute to see the valid units")
            else:
                #replace single quotation marks with two of them
                if "\'" in unit:
                    unit = unit.replace("'", "''")  

                #enclose in single quotation marks
                unit = '\''+unit+'\''
                return unit
        
    def _checkColumnForSingleInputFunction(self, columnByUser, functionName,
                                                  validInputTypes):
        """
        This method returns a column definition to be used as argument for a 
        DB2GSE's function. See _singleInputFunctionHandler()
        
        Returns
        -------
        str
            column definition if successful
        
        Raises
        ------
        IdaGeoDataFrameError
            If the IdaGeoDataFrame doesn't have at least one geometry column
        KeyError
            If columnByUser column is not in the IdaGeoDataFrame
        TypeError
            * If columnByUser column has incompatible type for the DB2GSE
              function
            * If the default geometry column has incompatible type for the
              DB2GSE function
            
        """
        columnName = ''
        if self._geometry is None:
            #When _geometry is none, means that the IdaGeoDataFrame doesn't
            #have even one geometry column
            raise IdaGeoDataFrameError("The IdaDataFrame doesn't have a "+
            "geometry column")
        elif columnByUser is not None:
            #User specified column name when calling the method
            if columnByUser not in self.columns:
                #User-specified column is not present
                raise KeyError("Column '"+columnByUser+"' not in "+
                self._name)
            elif self.dtypes.TYPENAME[columnByUser] in validInputTypes:
                #User-specified column has compatible type
                columnName = columnByUser
            elif (validInputTypes[0] == 'ST_GEOMETRY' and 
            columnByUser in self._dtypesGeometrical().index):
                #if the DB2GSE function accepts ST_GEOMETRY type, it's enough
                #if the specified column has any geometry type
                columnName = columnByUser
            else:
                #User-specified column has incompatible geometry type
                raise TypeError("Column '"+columnByUser+"' has "+
                "incompatible type.\nCompatible types are: "+
                ",".join(elem for elem in validInputTypes))
                print("Hint:\n"+
                "dtypes attribute shows the column names and their types\n")
        else:
            #User didn't specify a column, try to use default geometry column
            if self.dtypes.TYPENAME[self._geometry] in validInputTypes:
                #Default geometry column has compatible geometry type
                columnName = self._geometry
            elif (validInputTypes[0] == 'ST_GEOMETRY' and 
            self._geometry in self._dtypesGeometrical().index):
                #if the DB2GSE function accepts ST_GEOMETRY type
                #checking if the default geometry column is in
                #_dtypesGeometrical() is a mere sanity check
                columnName = self._geometry
            else:
                ##Default geometry column has incompatible geometry type
                raise TypeError("Default geometry column '"+
                self._geometry+"' has incompatible type.\nCompatible types "+
                "are: "+",".join(elem for elem in validInputTypes))
                print("Hint:\n"+
                "dtypes attribute shows the column names and their types\n")
                print("Hint:\n"+
                "specify a column name when calling the method or "+
                "change the default geometry column with setGeometry() method")
                
        
        
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
                                    validInputTypes=['ST_GEOMETRY']):
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
        validInputTypes : list
            List of the valid datatypes for the given DB2GSE function. 
            
        Raises
        ------
        IdaGeoDataFrameError
            See _checkColumnForSingleInputFunction() method
        
        """
                
        newGeoidadf = self._clone()
        workingColumn = self._checkColumnForSingleInputFunction(columnByUser,
                                                            functionName,
                                                            validInputTypes)                                                         

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
    