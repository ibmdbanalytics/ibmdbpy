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
Test module for IdaGeoSeries
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

from ibmdbpy import IdaSeries
from ibmdbpy import IdaGeoSeries
from ibmdbpy.exceptions import IdaGeoDataFrameError


class Test_IdaGeoSeries(object):

    def test_idageoseries_generalize(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE']
        with pytest.raises(TypeError):
            idageoseries.generalize('not a float')
        with pytest.raises(ValueError):
            idageoseries.generalize(-5)
        assert(isinstance(idageoseries.generalize(1.0), IdaGeoSeries))

    def test_idageoseries_buffer(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE']
        with pytest.raises(TypeError):
            idageoseries.buffer(distance='not a number')
        assert(isinstance(idageoseries.buffer(
                distance=2.3, unit="CLARKE'S FOOT"), IdaGeoSeries))

    def test_idageoseries_centroid(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE']
        assert(isinstance(idageoseries.centroid(), IdaGeoSeries))

    def test_idageoseries_convex_hull(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE']
        assert(isinstance(idageoseries.convex_hull(), IdaGeoSeries))

    def test_idageoseries_boundary(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE']
        assert(isinstance(idageoseries.boundary(), IdaGeoSeries))

    def test_idageoseries_envelope(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE']
        assert(isinstance(idageoseries.envelope(), IdaGeoSeries))

    def test_idageoseries_exterior_ring(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE'].envelope() # get ST_POLYGON
        assert(isinstance(idageoseries.exterior_ring(), IdaGeoSeries))

    def test_idageoseries_mbr(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE']
        assert(isinstance(idageoseries.mbr(), IdaGeoSeries))

    def test_idageoseries_end_point(self, idageodf_county):
        # TODO: add dataset with a column with ST_LINESTRING
        pass

    def test_idageoseries_mid_point(self, idageodf_county):
        # TODO: add dataset with a column with ST_LINESTRING
        pass

    def test_idageoseries_start_point(self, idageodf_county):
        # TODO: add dataset with a column with ST_LINESTRING
        pass

    def test_idageoseries_srid(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE']
        assert(isinstance(idageoseries.srid(), IdaSeries))

    def test_idageoseries_srs_name(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE']
        assert(isinstance(idageoseries.srs_name(), IdaSeries))

    def test_idageoseries_geometry_type(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE']
        assert(isinstance(idageoseries.geometry_type(), IdaSeries))

    def test_idageoseries_area(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE']
        assert(isinstance(idageoseries.area(
                unit="BRITISH FOOT (BENOIT 1895 A)"), IdaSeries))

    def test_idageoseries_dimension(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE']
        assert(isinstance(idageoseries.dimension(), IdaSeries))  

    def test_idageoseries_length(self, idageodf_tornado):
        idageoseries = idageodf_tornado['SHAPE']
        assert(isinstance(idageoseries.length(
                unit="CLARKE'S FOOT"), IdaSeries))

    def test_idageoseries_perimeter(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE']
        assert(isinstance(idageoseries.perimeter(
                unit="CLARKE'S FOOT"), IdaSeries))

    def test_idageoseries_num_geometries(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE']
        assert(isinstance(idageoseries.num_geometries(), IdaSeries))

    def test_idageoseries_num_interior_ring(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE'].envelope() # get ST_POLYGON
        assert(isinstance(idageoseries.num_interior_ring(), IdaSeries))

    def test_idageoseries_num_line_strings(self, idageodf_tornado):
        idageoseries = idageodf_tornado['SHAPE']
        assert(isinstance(idageoseries.num_line_strings(), IdaSeries))

    def test_idageoseries_num_points(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE']
        assert(isinstance(idageoseries.num_points(), IdaSeries))

    def test_idageoseries_num_polygons(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE']
        assert(isinstance(idageoseries.num_polygons(), IdaSeries))

    def test_idageoseries_coord_dim(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE']
        assert(isinstance(idageoseries.coord_dim(), IdaSeries))

    def test_idageoseries_is_3d(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE']
        assert(isinstance(idageoseries.is_3d(), IdaSeries))

    def test_idageoseries_is_measured(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE']
        assert(isinstance(idageoseries.is_measured(), IdaSeries))

    def test_idageoseries_is_valid(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE']
        assert(isinstance(idageoseries.is_valid(), IdaSeries))
        
    def test_idageoseries_max_m(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE']
        assert(isinstance(idageoseries.max_m(), IdaSeries))

    def test_idageoseries_max_x(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE']
        assert(isinstance(idageoseries.max_x(), IdaSeries))

    def test_idageoseries_max_y(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE']
        assert(isinstance(idageoseries.max_y(), IdaSeries))

    def test_idageoseries_max_z(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE']
        assert(isinstance(idageoseries.max_z(), IdaSeries))
        
    def test_idageoseries_min_m(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE']
        assert(isinstance(idageoseries.min_m(), IdaSeries))

    def test_idageoseries_min_x(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE']
        assert(isinstance(idageoseries.min_x(), IdaSeries))

    def test_idageoseries_min_y(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE']
        assert(isinstance(idageoseries.min_y(), IdaSeries))

    def test_idageoseries_min_z(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE']
        assert(isinstance(idageoseries.min_z(), IdaSeries))

    def test_idageoseries_m(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE'].centroid()
        assert(isinstance(idageoseries.m(), IdaSeries))

    def test_idageoseries_x(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE'].centroid()
        assert(isinstance(idageoseries.x(), IdaSeries))

    def test_idageoseries_y(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE'].centroid()
        assert(isinstance(idageoseries.y(), IdaSeries))

    def test_idageoseries_z(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE'].centroid()
        assert(isinstance(idageoseries.z(), IdaSeries))

    def test_idageoseries_is_closed(self, idageodf_county):
        # TODO: add dataset with a column with ST_LINESTRING
        pass

    def test_idageoseries_is_empty(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE']
        assert(isinstance(idageoseries.is_empty(), IdaSeries))

    def test_idageoseries_is_simple(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE']
        assert(isinstance(idageoseries.is_simple(), IdaSeries))
    
    def test_idageoseries_check_linear_unit(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE']
        with pytest.raises(TypeError):
            idageoseries._check_linear_unit(10)
        with pytest.raises(IdaGeoDataFrameError):
            idageoseries._check_linear_unit('not a valid unit')
        unit = 'meter'
        ans = idageoseries._check_linear_unit(unit)
        assert(ans == '\'METER\'')
        unit = 'Yard (SEARS)' # parenthesis
        ans = idageoseries._check_linear_unit(unit)
        assert(ans == '\'YARD (SEARS)\'')
        unit = 'Clarke\'s foot' # quotation mark
        ans = idageoseries._check_linear_unit(unit)
        assert(ans == '\'CLARKE\'\'S FOOT\'')
        unit = 'bin width 37.5 METRES' #with dot    
        ans = idageoseries._check_linear_unit(unit)
        assert(ans == '\'BIN WIDTH 37.5 METRES\'')
    
    def test_idageoseries_linear_units(self, idageodf_county):
        idageoseries = idageodf_county['SHAPE']
        units = idageoseries.linear_units
        assert(not units.empty)

    def test_idageoseries_unary_operation_handler_non_geometry_column(
            self, idageodf_county):
        idageoseries = idageodf_county['SHAPE'] # ST_POLYGON      
        with pytest.raises(TypeError):
            idageoseries._unary_operation_handler(
                db2gse_function = 'DB2GSE.ST_AGEOSPATIALFUNCTION',
                valid_types = ['ST_POINT'])

    