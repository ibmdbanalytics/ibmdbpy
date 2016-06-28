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
Test module for non-private methods of IdaGeoDataFrame
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

class Test_IdaGeoDataFrame_WrappersForGeospatialMethod(object):

    def test_idageodf_fromIdaDataFrame_default(self, idadf, idageodf_county):
        newidageodf = IdaGeoDataFrame.fromIdaDataFrame(idadf)
        assert(isinstance(newidageodf, IdaGeoDataFrame))
        newidageodf = IdaGeoDataFrame.fromIdaDataFrame(idageodf_county)
        assert(isinstance(newidageodf, IdaGeoDataFrame))
        
    def test_idageodf_fromIdaDataFrame_TypeError(self):
        aString = "a string"
        with pytest.raises(TypeError):            
            IdaGeoDataFrame.fromIdaDataFrame(aString)
            
    def test_idageodf_getitem_1_col_returnsIdaGeoDataFrame(self, idageodf_county):
        colName = idageodf_county.columns[0]
        ida = idageodf_county[colName]
        assert(isinstance(ida, IdaGeoDataFrame))
        assert(len(ida.columns) == 1)
        assert(ida.columns[0] == idageodf_county.columns[0])
            
    def test_idageodf_buffer_TypeError(self, idageodf_county):
        with pytest.raises(TypeError):
            idageodf_county.buffer() #missing distance

    def test_idageodf_buffer(self, idageodf_county):
        ans = idageodf_county.buffer(distance=10.2, unit="Clarke's foot")
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'ST_GEOMETRY')
        
    def test_idageodf_centroid(self, idageodf_county):
        ans = idageodf_county.centroid()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'ST_POINT')
    
    def test_idageodf_boundary(self, idageodf_county):
        ans = idageodf_county.boundary()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'ST_GEOMETRY')
    
    def test_idageodf_envelope(self, idageodf_county):
        ans = idageodf_county.envelope()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'ST_POLYGON')
        
    def test_idageodf_MBR(self, idageodf_county):
        ans = idageodf_county.MBR()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'ST_GEOMETRY')    
    
    def test_idageodf_SRID(self, idageodf_county):
        ans = idageodf_county.SRID()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'INTEGER')
    
    def test_idageodf_srsName(self, idageodf_county):
        ans = idageodf_county.srsName()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'VARCHAR')      
    
    def test_idageodf_geometryType(self, idageodf_county):
        ans = idageodf_county.geometryType()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'VARCHAR')  
    
    def test_idageodf_area(self, idageodf_county):
        ans = idageodf_county.area()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'DOUBLE')
    
    def test_idageodf_dimension(self, idageodf_county):
        ans = idageodf_county.dimension()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'INTEGER')
    
    def test_idageodf_length(self, idageodf_tornado):
        ans = idageodf_tornado.length()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'DOUBLE')
        
    def test_idageodf_perimeter(self, idageodf_county):
        ans = idageodf_county.perimeter()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'DOUBLE')        
    
    def test_idageodf_numGeometries(self, idageodf_county):
        ans = idageodf_county.numGeometries()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'INTEGER')  
    
    def test_idageodf_numInteriorRing(self, idageodf_county):        
        idageodf = idageodf_county._clone()
        #use envelope to create a column with ST_POLYGON type
        idageodf['SHAPE'] = idageodf_county.envelope()
        ans = idageodf.numInteriorRing()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'INTEGER')
    
    def test_idageodf_numLineStrings(self, idageodf_tornado):
        ans = idageodf_tornado.numLineStrings()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'INTEGER')
        
    def test_idageodf_numPoints(self, idageodf_county):
        ans = idageodf_county.numPoints()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'INTEGER')
    
    def test_idageodf_numPolygons(self, idageodf_county):
        ans = idageodf_county.numPolygons()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'INTEGER')
    
    def test_idageodf_coordDim(self, idageodf_county):
        ans = idageodf_county.coordDim()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'INTEGER')
    
    def test_idageodf_is3d(self, idageodf_county):
        ans = idageodf_county.is3d()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'INTEGER')
    
    def test_idageodf_isMeasured(self, idageodf_county):
        ans = idageodf_county.isMeasured()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'INTEGER')
    
    def test_idageodf_isValid(self, idageodf_county):
        ans = idageodf_county.isValid()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'INTEGER')
    
    def test_idageodf_maxM(self, idageodf_county):
        ans = idageodf_county.maxM()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'DOUBLE')
    
    def test_idageodf_maxX(self, idageodf_county):
        ans = idageodf_county.maxX()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'DOUBLE')
    
    def test_idageodf_maxY(self, idageodf_county):
        ans = idageodf_county.maxY()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'DOUBLE')
        
    def test_idageodf_maxZ(self, idageodf_county):
        ans = idageodf_county.maxZ()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'DOUBLE')
    
    def test_idageodf_minM(self, idageodf_county):
        ans = idageodf_county.minM()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'DOUBLE')
    
    def test_idageodf_minX(self, idageodf_county):
        ans = idageodf_county.minX()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'DOUBLE')
    
    def test_idageodf_minY(self, idageodf_county):
        ans = idageodf_county.minY()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'DOUBLE')
    
    def test_idageodf_minZ(self, idageodf_county):
        ans = idageodf_county.minZ()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'DOUBLE')
    
    def test_idageodf_M(self, idageodf_customer):
        ans = idageodf_customer.M()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'DOUBLE')
    
    def test_idageodf_X(self, idageodf_customer):
        ans = idageodf_customer.X()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'DOUBLE')
    
    def test_idageodf_Y(self, idageodf_customer):
        ans = idageodf_customer.Y()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'DOUBLE')
    
    def test_idageodf_Z(self, idageodf_customer):
        ans = idageodf_customer.Z()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'DOUBLE')
    
    def test_idageodf_isClosed(self, idageodf_tornado):
        ans = idageodf_tornado.isClosed()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'INTEGER')
    
    def test_idageodf_isEmpty(self, idageodf_county):
        ans = idageodf_county.isEmpty()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'INTEGER')
    
    def test_idageodf_isSimple(self, idageodf_county):
        ans = idageodf_county.isSimple()
        assert(isinstance(ans, IdaGeoDataFrame))
        assert(ans.dtypes.TYPENAME[ans._geometry] == 'INTEGER')
    
    def test_idageodf_linearUnits(self, idageodf_county):
        ans = idageodf_county.linearUnits
        assert(isinstance(ans, pandas.Series))
    
    def test_idageodf_setGeometry_correctColumn(self, idageodf_county):
        #Column 'SHAPE' in idageodf_county has geometry type
        idageodf_county.setGeometry('SHAPE')
        assert(idageodf_county._geometry == 'SHAPE')
    
    def test_idageodf_setGeometry_KeyError(self, idageodf_county):
        with pytest.raises(KeyError):
            idageodf_county.setGeometry('this is not a column')    
