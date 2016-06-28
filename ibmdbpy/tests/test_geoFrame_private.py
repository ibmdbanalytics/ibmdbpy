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
Test module for private methods of IdaGeoDataFrame
"""
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()

import pandas
import pytest
import six

from ibmdbpy import IdaGeoDataFrame
from ibmdbpy.exceptions import IdaGeoDataFrameError


class Test_IdaGeoDataFrame_PrivateMethods(object):
    
    def test_idageodf_clone(self, idageodf_county):
        cloned = idageodf_county._clone()
        assert(isinstance(cloned, IdaGeoDataFrame))
        assert(all(cloned.columns.tolist()) == all(idageodf_county.columns.tolist()))
        assert(all(cloned.dtypes) == all(idageodf_county.dtypes))
        assert(cloned.internal_state.name == idageodf_county.internal_state.name)
        assert(cloned.internal_state.ascending == idageodf_county.internal_state.ascending)
        assert(all(cloned.internal_state._views) == all(idageodf_county.internal_state._views))
        assert(all(cloned.internal_state._cumulative) == all(idageodf_county.internal_state._cumulative))
        assert(cloned.internal_state.order == idageodf_county.internal_state.order)
        assert(cloned.internal_state.columndict == idageodf_county.internal_state.columndict)

    def test_idageodf_dtypesGeometrical(self, idageodf_county):
        df = idageodf_county._dtypesGeometrical()
        assert(isinstance(df, pandas.DataFrame))
        assert(all(df.columns.tolist()) == all(['TYPENAME', 'COLNUMBER']))
        for dataType in df['TYPENAME']:
            if dataType[0:3] != 'ST_':
                raise ValueError("Unexpected name of non-geometry column")
                
    def test_idageodf_checkLinearUnit_TypeError(self, idageodf_county):
        with pytest.raises(TypeError):
            idageodf_county._checkLinearUnit() #missing unit
        with pytest.raises(TypeError):
            idageodf_county._checkLinearUnit(1.6180) #not a string
    
    def test_idageodf_checkLinearUnit_IdaGeoDataFrameError(self, idageodf_county):
        with pytest.raises(IdaGeoDataFrameError):
            idageodf_county._checkLinearUnit(unit='not a unit')
    
    def test_idageodf_checkLinearUnit_default(self, idageodf_county):
        unit = "Clarke's foot"
        formattedUnit = unit.replace('\'','\'\'')
        formattedUnit = formattedUnit.upper()
        formattedUnit = '\''+formattedUnit+'\''
        returnedUnit = idageodf_county._checkLinearUnit(unit)
        assert(returnedUnit == formattedUnit)

    def test_idageodf_checkColumnForSingleInputFunction_IdaGeoDataFrameError_noGeometryColumnInIdaGeoDataFrame(self, 
                                                                                                               idageodf_county):
        idageodf = idageodf_county._clone()
        idageodf._geometry = None
        with pytest.raises(IdaGeoDataFrameError):
            idageodf._checkColumnForSingleInputFunction(columnByUser=None, 
                                                        functionName='functionName()', 
                                                        validInputTypes=['ST_GEOMETRY'])
            
    def test_idageodf_checkColumnForSingleInputFunction_KeyError_userEnteredColumnNotInIdaGeoDataFrame(self, 
                                                                                                       idageodf_county):
        with pytest.raises(KeyError):
            idageodf_county._checkColumnForSingleInputFunction(columnByUser='colname not in the IdaGeoDf', 
                                                               functionName='functionName()', 
                                                               validInputTypes=['ST_GEOMETRY'])
    
    def test_idageodf_checkColumnForSingleInputFunction_userEnteredColumnCorrect_validTypesIsASpecificGeometry(self, 
                                                                                                               idageodf_county):
        colDefinition = idageodf_county._checkColumnForSingleInputFunction(columnByUser='SHAPE', 
                                                                           functionName='functionName()', 
                                                                           validInputTypes=['ST_MULTIPOLYGON'])
        assert(isinstance(colDefinition, six.string_types))
        assert(colDefinition[0] != '\"' and colDefinition[-1] != '\"')
    
    def test_idageodf_checkColumnForSingleInputFunction_userEnteredColumnCorrect_validTypesIsAnyGeometry(self, 
                                                                                                         idageodf_county):
        colDefinition = idageodf_county._checkColumnForSingleInputFunction(columnByUser='SHAPE', 
                                                                           functionName='functionName()', 
                                                                           validInputTypes=['ST_GEOMETRY'])
        assert(isinstance(colDefinition, six.string_types))
        assert(colDefinition[0] != '\"' and colDefinition[-1] != '\"')
            
    def test_idageodf_checkColumnForSingleInputFunction_TypeError_userEnteredColumnIncompatibleType(self, 
                                                                                                    idageodf_county):
        with pytest.raises(TypeError):
            idageodf_county._checkColumnForSingleInputFunction(columnByUser='OBJECTID', 
                                                               functionName='functionName()', 
                                                               validInputTypes=['ST_GEOMETRY'])
        with pytest.raises(TypeError):
            #column 'SHAPE' of idageodf_county has ST_MULTIPOLYGON type
            idageodf_county._checkColumnForSingleInputFunction(columnByUser='SHAPE', 
                                                               functionName='functionName()', 
                                                               validInputTypes=['ST_LINESTRING'])
            
    def test_idageodf_checkColumnForSingleInputFunction_defaultGeometryColumnCorrect_validTypesIsASpecificGeometry(self, 
                                                                                                                   idageodf_county):
        colDefinition = idageodf_county._checkColumnForSingleInputFunction(columnByUser=None, 
                                                                           functionName='functionName()', 
                                                                           validInputTypes=['ST_MULTIPOLYGON'])
        assert(isinstance(colDefinition, six.string_types))
        assert(colDefinition[0] != '\"' and colDefinition[-1] != '\"')

    def test_idageodf_checkColumnForSingleInputFunction_defaultGeometryColumnCorrect_validTypesIsAnyGeometry(self, 
                                                                                                             idageodf_county):                                 
        colDefinition = idageodf_county._checkColumnForSingleInputFunction(columnByUser=None, 
                                                                           functionName='functionName()', 
                                                                           validInputTypes=['ST_GEOMETRY'])
        assert(isinstance(colDefinition, six.string_types))
        assert(colDefinition[0] != '\"' and colDefinition[-1] != '\"')
        
    def test_idageodf_checkColumnForSingleInputFunction_TypeError_defaultGeometryColumnIncompatibleType(self, 
                                                                                                        idageodf_county):
        #column 'SHAPE' of idageodf_county has ST_MULTIPOLYGON type
        idageodf = idageodf_county._clone()
        idageodf._geometry = 'SHAPE'
        with pytest.raises(TypeError):
            idageodf._checkColumnForSingleInputFunction(columnByUser=None, 
                                                        functionName='functionName()', 
                                                        validInputTypes=['ST_LINESTRING'])

    def test_idageodf_setGeometryAutomatically_noGeometryInIdaGeoDataFrame(self,
 idadf):
        idageodf = IdaGeoDataFrame.fromIdaDataFrame(idadf)
        idageodf._setGeometryAutomatically(verbose = False)
        assert(idageodf._geometry == None)        
    
    def test_idageodf_setGeometryAutomatically_default(self, idageodf_county):
        #idageodf_county has only one geometry column, namely: 'SHAPE'
        idageodf = idageodf_county._clone()
        idageodf._setGeometryAutomatically()        
        assert(idageodf._geometry == 'SHAPE')
    
    def _singleInputFunctionHandler(self, idageodf_county):
        ans = idageodf_county._singleInputFunctionHandler(functionName='ST_SOMETHING()', 
                                                          columnByUser='SHAPE',
                                                          additionalArguments=None, 
                                                          onlyTheseTypes=None)
        assert(isinstance(ans, IdaGeoDataFrame))
    