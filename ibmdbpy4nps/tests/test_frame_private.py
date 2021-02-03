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
Test module for private methods of IdaDataFrame
"""
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()

import pandas

class Test_IdaDataFrame_PrivateMethods(object):
    def test_idadf_clone(self, idadf):
        pass

    def test_idadf_clone_as_series(self, idadf):
        pass

    def test_idadf_get_type(self, idadf):
        pass

    def test_idadf_get_columns(self, idadf):
        pass

    def test_idadf_get_all_columns_in_table(self, idadf):
        pass


    def test_idadf_get_index(self, idadf):
        pass

    def test_idadf_get_shape(self, idadf):
        pass

    def test_idadf_get_columns_dtypes(self, idadf):
        pass

    def test_idadf_table_def(self, idadf):
        to_assert = idadf._table_def()
        assert isinstance(to_assert, pandas.core.frame.DataFrame)
        assert to_assert.shape[0] == len(idadf.columns)
        assert to_assert.shape[1] == 3

    def test_idadf_get_numerical_columns(self, idadf):
        pass

    def test_idadf_reset_attributes(self, idadf):
        pass

    def test_combine_check(self, idadf):
        pass

    def test_idadf_prepare_and_execute(self, idadf):
        pass

    def test_idadf_autocommit(self, idadf):
        pass

    def test_idadf_check_connection(self, idadf):
        pass

    def test_idadf_numeric_stats(self, idadf):
        pass

    def test_idadf_get_percentiles(self, idadf):
        pass

    def test_idadf_categorical_stats(self, idadf):
        pass

    def test_idadf_get_number_of_nas(self, idadf):
        pass

    def test_idadf_count_level(self, idadf):
        pass

    def test_idadf_counbt_level_groupby(self, idadf):
        pass

    def test_idadf_factors_count(self, idadf):
        pass

    def test_idadf_factors_sum(self, idadf):
        pass

    def test_idadf_factors_avg(self, idadf):
        pass