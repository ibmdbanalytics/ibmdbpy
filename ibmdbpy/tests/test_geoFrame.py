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
Test module for IdaGeoDataFrame
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

from copy import deepcopy

from ibmdbpy import IdaDataFrame
from ibmdbpy import IdaSeries
from ibmdbpy import IdaGeoDataFrame
from ibmdbpy import IdaGeoSeries

class Test_IdaGeoDataFrame(object):

    def test_idageodf_set_geometry_error(self, idageodf_county):
        with pytest.raises(KeyError):
            idageodf_county.set_geometry('not a column in the Ida')
        with pytest.raises(TypeError):
            idageodf_county.set_geometry('OBJECTID')

    def test_idageodf_set_geometry_success(self, idageodf_county):
        geometry_colname = 'SHAPE'
        idageodf_county.set_geometry(geometry_colname)
        assert(isinstance(idageodf_county.geometry, IdaGeoSeries))
        assert(idageodf_county.geometry.column == geometry_colname)

    def test_idageodf_nondestructive_geometry_column_deletion(
            self, idageodf_county):
        geometry_colname = 'SHAPE'
        idageodf_county.set_geometry(geometry_colname)
        assert(idageodf_county.geometry.column == geometry_colname)
        del(idageodf_county[geometry_colname])
        with pytest.raises(AttributeError):
            idageodf_county.geometry

    def test_idageodf_geometry_not_set(self, idageodf_county):
        idageodf_county._geometry_colname = None
        with pytest.raises(AttributeError):
            idageodf_county.geometry

    def test_idageodf_fromIdaDataFrame(self, idadf):
        newidageodf = IdaGeoDataFrame.from_IdaDataFrame(idadf)
        assert(isinstance(newidageodf, IdaGeoDataFrame))

    def test_idageodf_column_projection_IdaGeoSeries(self, idageodf_county):
        indexer = 'OBJECTID'
        idageodf_county.indexer = indexer
        column = 'SHAPE'
        ida = idageodf_county[column]
        assert(isinstance(ida, IdaGeoSeries))
        assert(ida.column == column)
        assert(ida.indexer == indexer)

    def test_idageodf_column_projection_IdaSeries(self, idageodf_county):
        indexer = 'OBJECTID'
        idageodf_county.indexer = indexer
        column = 'NAME'
        ida = idageodf_county[column]
        assert(isinstance(ida, IdaSeries))
        assert(ida.column == column)
        assert(ida.indexer == indexer)

    def test_idageodf_column_projection_IdaGeoDataFrame(self, idageodf_county):
        geometry_colname = 'SHAPE'
        idageodf_county.set_geometry(geometry_colname)
        columns = ['SHAPE', 'OBJECTID']
        ida = idageodf_county[columns]
        assert(isinstance(ida, IdaGeoDataFrame))
        assert(all(ida.columns) == all(columns))
        assert(ida.geometry.column == geometry_colname)

    def test_idageodf_column_projection_IdaDataFrame(self, idageodf_county):
        geometry_colname = 'SHAPE'
        idageodf_county.set_geometry(geometry_colname)
        columns = ['NAME', 'OBJECTID']
        ida = idageodf_county[columns]
        assert(isinstance(ida, IdaDataFrame))
        assert(all(ida.columns) == all(columns))
        with pytest.raises(AttributeError):
            ida.geometry

    def test_idageodf_geospatial_method_call_carried_on_IdaGeoSeries(
            self, idageodf_county):
        geometry_colname = 'SHAPE'
        idageodf_county.set_geometry(geometry_colname)
        attribute = 'area'
        assert(not hasattr(IdaGeoDataFrame, attribute))
        assert(hasattr(IdaGeoSeries, attribute))
        assert(idageodf_county.__getattr__(attribute))

    def test_idageodf_getattr_unresolved(self, idageodf_county):
        with pytest.raises(AttributeError):
            idageodf_county.__getattr__('not_an_attribute')

    def test_idageodf_equals(self, idageodf_customer, idageodf_county):
        idageodf = idageodf_customer
        idageodf.set_geometry('SHAPE')
        idageodf_county.set_geometry('SHAPE')
        assert (isinstance(idageodf.equals(idageodf_county), IdaGeoDataFrame))

    def test_idageodf_distance(self, idageodf_customer,idageodf_county):
        idageodf = idageodf_customer
        idageodf.set_geometry('SHAPE')
        idageodf_county.set_geometry('SHAPE')
        assert (isinstance(idageodf.distance(idageodf_county), IdaGeoDataFrame))

    def test_idageodf_crosses(self,idageodf_customer,idageodf_county):
        idageodf = idageodf_customer
        idageodf.set_geometry('SHAPE')
        idageodf_county.set_geometry('SHAPE')
        assert (isinstance(idageodf.crosses(idageodf_county), IdaGeoDataFrame))

    def test_idageodf_intersects(self, idageodf_customer, idageodf_county):
        idageodf = idageodf_customer
        idageodf.set_geometry('SHAPE')
        idageodf_county.set_geometry('SHAPE')
        assert (isinstance(idageodf.intersects(idageodf_county), IdaGeoDataFrame))

    def test_idageodf_overlaps(self, idageodf_customer, idageodf_county):
        idageodf = idageodf_customer
        idageodf.set_geometry('SHAPE')
        idageodf_county.set_geometry('SHAPE')
        assert (isinstance(idageodf.overlaps(idageodf_county), IdaGeoDataFrame))

    def test_idageodf_touches(self, idageodf_customer, idageodf_county):
        idageodf = idageodf_customer
        idageodf.set_geometry('SHAPE')
        idageodf_county.set_geometry('SHAPE')
        assert (isinstance(idageodf.touches(idageodf_county), IdaGeoDataFrame))

    def test_idageodf_disjoint(self, idageodf_customer, idageodf_county):
        idageodf = idageodf_customer
        idageodf.set_geometry('SHAPE')
        idageodf_county.set_geometry('SHAPE')
        assert (isinstance(idageodf.disjoint(idageodf_county), IdaGeoDataFrame))

    def test_idageodf_contains(self, idageodf_customer,idageodf_county):
        idageodf = idageodf_customer
        idageodf.set_geometry('SHAPE')
        idageodf_county.set_geometry('SHAPE')
        assert (isinstance(idageodf.contains(idageodf_county), IdaGeoDataFrame))

    def test_idageodf_within(self, idageodf_customer, idageodf_county):
        idageodf = idageodf_customer
        idageodf.set_geometry('SHAPE')
        idageodf_county.set_geometry('SHAPE')
        assert (isinstance(idageodf.within(idageodf_county), IdaGeoDataFrame))

    def test_idageodf_mbr_intersects(self, idageodf_customer, idageodf_county):
        idageodf = idageodf_customer
        idageodf.set_geometry('SHAPE')
        idageodf_county.set_geometry('SHAPE')
        assert (isinstance(idageodf.mbr_intersects(idageodf_county), IdaGeoDataFrame))

    def test_idageodf_difference(self, idageodf_customer,idageodf_county):
        idageodf = idageodf_customer
        idageodf.set_geometry('SHAPE')
        idageodf_county.set_geometry('SHAPE')
        assert (isinstance(idageodf.difference(idageodf_county), IdaGeoDataFrame))

    def test_idageodf_intersection(self, idageodf_customer,idageodf_county):
        idageodf = idageodf_customer
        idageodf.set_geometry('SHAPE')
        idageodf_county.set_geometry('SHAPE')
        assert (isinstance(idageodf.intersection(idageodf_county), IdaGeoDataFrame))

    def test_idageodf_union(self, idageodf_customer,idageodf_county):
        idageodf = idageodf_customer
        idageodf.set_geometry('SHAPE')
        idageodf_county.set_geometry('SHAPE')
        assert (isinstance(idageodf.union(idageodf_county), IdaGeoDataFrame))

    def test_idageodf_binary_operation_handler_non_geometry_column(
            self, idageodf_customer,idageodf_county):
        idageodf = idageodf_customer  # ST_POLYGON
        with pytest.raises(TypeError):
            idageodf._binary_operation_handler(
                idageodf_county,
                db2gse_function='DB2GSE.ST_AGEOSPATIALFUNCTION',
                valid_types=['ST_POINT'])