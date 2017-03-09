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
Test module for indexing of IdaDataFrameObjects
"""
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()

import ibmdbpy

class Test_projection(object):

    def test_projection_1_col_idadf(self, idadf):
        col = idadf.columns[0]
        ida = idadf[[col]]
        assert(type(ida) == ibmdbpy.frame.IdaDataFrame)
        assert(len(ida.columns) == 1)
        head = ida.head()
        assert (head.name == idadf.columns[0])
        pass

    def test_projection_1_col_idaseries(self, idadf):
        col = idadf.columns[0]
        ida = idadf[col]
        assert(type(ida) == ibmdbpy.series.IdaSeries)
        assert(len(ida.columns) == 1)
        assert(ida.column == idadf.columns[0])
        head = ida.head()
        assert(head.name == idadf.columns[0])
        pass

    def test_projection_1_col_loc(idadf):
        pass

    def test_projection_list_of_col(idadf):
        pass

    def test_projection_list_of_col_loc(idadf):
        pass

    def test_projection_error(idadf):
        pass


class Test_selection(object):

    def test_selection_1_row(idadf):
        pass

    def test_selection_list_of_rows(idadf):
        pass

    def test_selection_slice_of_rows(idadf):
        pass

    def test_selection_slice_error(idadf):
        pass


class Test_projection_selection(object):

    def test_selection_1_row_projection_1_col_idadf(idadf):
        pass

    def test_selection_1_row_projection_1_col_idaseries(idadf):
        pass

    def test_selection_1_row_projection_list_of_col(idadf):
        pass

    def test_selection_list_of_rows_projection_1_col_idadf(idadf):
        pass

    def test_selection_list_of_rows_projection_1_col_idaseries(idadf):
        pass

    def test_selection_list_of_rows_projection_list_of_col(idadf):
        pass

    def test_selection_slice_of_rows_projection_1_col_idadf(idadf):
        pass

    def test_selection_slice_of_rows_projection_1_col_idaseries(idadf):
        pass

    def test_selection_slice_of_rows_projection_list_of_col(idadf):
        pass

    def test_selection_projection_error(idadf):
        pass

class Test_Loc(object):

    def test_loc_error(idadf):
        pass