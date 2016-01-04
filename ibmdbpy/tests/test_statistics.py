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
Test module for statistics for IdaDataFrame Objects
"""

from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from builtins import object
from builtins import str
from future import standard_library
standard_library.install_aliases()

import numpy
import pandas
import pytest

from ibmdbpy.statistics import _numeric_stats , _get_percentiles, _get_number_of_nas, _count_level, _count_level_groupby

class Test_PrivateStatisticsMethods(object):

    def test_idadf_numeric_stats_default(self, idadf):
        data = idadf._table_def() # We necessarly have to put the test under this condition
        columns = list(data.loc[data['VALTYPE'] == "NUMERIC"].index)
        assert isinstance(_numeric_stats(idadf, "count", columns), numpy.ndarray)
        assert isinstance(_numeric_stats(idadf, "mean", columns), numpy.ndarray)
        assert isinstance(_numeric_stats(idadf, "median", columns), numpy.ndarray)
        assert isinstance(_numeric_stats(idadf, "std", columns), numpy.ndarray)
        assert isinstance(_numeric_stats(idadf, "var", columns), numpy.ndarray)
        assert isinstance(_numeric_stats(idadf, "min", columns), numpy.ndarray)
        assert isinstance(_numeric_stats(idadf, "max", columns), numpy.ndarray)

    def test_idadf_numeric_stats_accuracy(self, idadf):
        pass

    def test_idadf_get_percentiles_default(self, idadf):
        data = idadf._table_def() # We necessarly have to put the test under this condition
        columns = list(data.loc[data['VALTYPE'] == "NUMERIC"].index)
        assert isinstance(_get_percentiles(idadf, 0.5,columns), pandas.core.frame.DataFrame)
        assert isinstance(_get_percentiles(idadf, [0.5],columns), pandas.core.frame.DataFrame)
        assert _get_percentiles(idadf, [0.5],columns).shape[0] == 1
        assert isinstance(_get_percentiles(idadf, [0.3,0.4,0.5],columns), pandas.core.frame.DataFrame)
        assert _get_percentiles(idadf, [0.3,0.4,0.5],columns).shape[0] == 3

    def test_idadf_get_percentiles_accuracy(self, idadf):
        pass

    def test_idadf_get_categorical_stats(self, idadf):
        # FUNCTION TO IMPLEMENT
        pass

    def test_idadf_get_number_of_nas(self, idadf):
        assert isinstance(_get_number_of_nas(idadf, idadf.columns), tuple)
        assert len(_get_number_of_nas(idadf, idadf.columns)) == len(idadf.columns)
        assert len(_get_number_of_nas(idadf, idadf.columns[0])) == 1

    def test_idadf_get_number_of_nas_accuracy(self, idadf):
        pass



    def test_idadf_count_level(self, idadf):
        assert isinstance(_count_level(idadf), tuple)
        assert len(_count_level(idadf)) == len(idadf.columns)
        assert len(_count_level(idadf, idadf.columns)) == len(idadf.columns)

    def test_idadf_count_level_accuracy(self, idadf):
        pass

    def test_idadf_count_level_groupby(self, idadf):
        assert isinstance(_count_level_groupby(idadf), tuple)
        assert(_count_level_groupby(idadf)[0] % 1 == 0)
        assert len(_count_level_groupby(idadf)) == 1
        assert len(_count_level_groupby(idadf, idadf.columns)) == 1

    def test_idadf_count_level_group_by_accuracy(self, idadf):
        pass

    def test_idadf_factor_count(self, idadf):
        pass

    def test_idadf_factor_sum(self, idadf):
        pass

    def test_idadf_factor_avg(self, idadf):
        pass

class Test_DescriptiveStatistics(object):

    def test_idadf_pivot_table(self, idadf):
        pass

    def test_idadf_describe_default(self, idadf):
        to_assert = idadf.describe()
        assert isinstance(to_assert, pandas.core.frame.DataFrame)
        assert (len(to_assert) == 8) & (to_assert.shape[1] <= len(idadf.columns))
        assert all([x in to_assert.index for x in ["count","mean","std", "min", "max"]])

    def test_idadf_describe_custom_percentiles(self, idadf):
        assert len(idadf.describe([0.2,0.3,0.4,0.5,0.6])) == 10
        assert len(idadf.describe([0.5])) == 6

    def test_idadf_describe_bad_parameter_value(self, idadf):
        with pytest.raises(ValueError):
            idadf.describe([0.2,0.3,0.4,0.5,-1])

    def test_idadf_describe_bad_parameter_type(self, idadf):
        with pytest.raises(TypeError):
            idadf.describe("string")

    def test_idadf_quantile_default(self, idadf, df):
        assert str(idadf.quantile()) == str(df.quantile())

    def test_idadf_quantile_custom(self, idadf, df):
        assert all(idadf.quantile([0.2,0.4,0.6,0.8]) == df.quantile([0.2,0.4,0.6,0.8]))

    def test_idadf_quantile_value_out_of_range(self, idadf):
        with pytest.raises(ValueError):
            idadf.quantile([5])
        with pytest.raises(ValueError):
            idadf.quantile([-0.4])
        with pytest.raises(TypeError):
            idadf.quantile(["lol"])
        with pytest.raises(TypeError):
            idadf.quantile("lol")
        with pytest.raises(TypeError):
            idadf.quantile(0.5,0.7)

    def test_idadf_cov(self, idadf, df):
        assert str(idadf.cov()) == str(df.cov())

    def test_idadf_corr(self, idadf, df):
        assert str(idadf.corr()) == str(df.corr())

    def test_idadf_mad(self, idadf, df):
        assert str(idadf.mad()) == str(df.mad())

    # TODO : Fix min and max in python 2
    def test_idadf_min(self, idadf, df):
        assert str(idadf.min()) == str(df.min())

    def test_idadf_max(self, idadf, df):
        assert str(idadf.max()) == str(df.max())

    def test_idadf_count(self, idadf, df):
        assert all(idadf.count() == df.count())

    def test_idadf_count_distinct(self, idadf, df):
        pass

    def test_idadf_std(self, idadf, df):
        assert str(idadf.std()) == str(df.std())

    def test_idadf_var(self, idadf, df):
        assert str(idadf.var()) == str(df.var())

    def test_idadf_mean(self, idadf, df):
        assert str(idadf.mean()) == str(df.mean())

    def test_idadf_sum(self, idadf, df):
        assert str(idadf.sum()) == str(df.sum(numeric_only = True))

    def test_idadf_median(self, idadf, df):
        assert str(idadf.median()) == str(df.median())