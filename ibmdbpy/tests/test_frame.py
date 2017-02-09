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
Test module for IdaDataFrameObjects
"""
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from builtins import zip
from future import standard_library
standard_library.install_aliases()

import pandas
import pytest
import six

import ibmdbpy
from ibmdbpy import IdaDataBase


class Test_OpenDataFrameObject(object):

    def test_idadf_attr_idadb(self, idadf):
        assert isinstance(idadf._idadb, IdaDataBase)

    def test_idadf_attr_name(self, idadf, df):
        assert isinstance(idadf.name, six.string_types)
        assert idadf.name == idadf.schema + "." + "TEST_IBMDBPY"
        assert idadf.name == idadf.schema + "." + idadf.tablename

    def test_idadf_attr_schema(self, idadf):
        assert isinstance(idadf.schema, six.string_types)

    def test_idadf_attr_indexer(self, idadf):
        assert (isinstance(idadf.indexer, six.string_types)|(idadf.indexer is None))
        # TODO : Check more deeply the indexer

    def test_idadf_attr_loc(self, idadf):
        assert isinstance(idadf.loc, ibmdbpy.indexing.Loc)

    def test_idadf_attr_internalstate(self, idadf):
        assert isinstance(idadf.internal_state, ibmdbpy.internals.InternalState)

    def test_idadf_attr_type(self, idadf):
        assert isinstance(idadf.type, six.string_types)
        assert idadf.type == "Table"

    def test_idadf_atrr_dtypes(self, idadf, df):
        assert isinstance(idadf.dtypes, pandas.core.frame.DataFrame)
        assert len(idadf.dtypes) == len(idadf.columns)
        assert len(idadf.dtypes) == len(df.columns)

    def test_idadf_attr_index(self, idadf, df):
        # Ok, but what do we do if too big ?
        assert type(idadf.index) in [pandas.core.index.Int64Index,pandas.core.index.Index] # Not sure here
        assert list(idadf.index) == list(df.index)

    def test_idadf_attr_columns(self, idadf, df):
        assert isinstance(idadf.columns, pandas.core.index.Index)
        assert idadf.columns.equals(df.columns)

    def test_idadf_attr_axes(self, idadf):
        assert isinstance(idadf.axes, list)
        assert len(idadf.axes) == 2
        assert idadf.axes[1].equals(idadf.columns)
        assert list(idadf.axes[0]) == list(idadf.index)

    def test_idadf_attr_shape(self, idadf, df):
        assert isinstance(idadf.shape, tuple)
        assert len(idadf.shape) == 2
        assert idadf.shape[0] == len(idadf.index)
        assert idadf.shape[1] == len(idadf.columns)
        assert idadf.shape == df.shape

    def test_idadf_empty(self, idadb, df):
        idadb._create_table(df, "TEST_EMPTY_3496593727406047264076")
        to_test = ibmdbpy.IdaDataFrame(idadb, "TEST_EMPTY_3496593727406047264076")
        assert(to_test.empty is True)
        idadb.drop_table("TEST_EMPTY_3496593727406047264076")

    def test_idadf_len(self, idadf, df):
        assert(len(idadf) == len(df))

    def test_idadf_iter(self, idadf, df):
        for idacol, col in zip(idadf, df):
            assert(idacol == col)


class Test_IdaDataFrameBehavior(object):
    def test_idadf_getitem_1_col_idadf(self, idadf):
        if len(idadf.columns) >= 1:
            newidadf = idadf[[idadf.columns[0]]]
            assert(isinstance(newidadf, ibmdbpy.IdaDataFrame) is True)
            assert(len(newidadf.columns) == 1)
            assert(idadf.columns[0] == newidadf.columns[0])

            # We don't check of it is actually the corresponding column

            newidadf = idadf[[idadf.columns[-1]]]
            assert(isinstance(newidadf, ibmdbpy.IdaDataFrame) is True)
            assert(len(newidadf.columns) == 1)
            assert(idadf.columns[-1] == newidadf.columns[0])

    def test_idadf_getitem_1_col_idadf_keyerror(self, idadf):
        with pytest.raises(KeyError):
            idadf[["NOTEXISTING_COLUMN_455849820205"]]

    def test_idadf_getitem_2_cols_idadf(self, idadf):
        if len(idadf.columns) >= 2:
            newidadf = idadf[[idadf.columns[0], idadf.columns[-1]]]
            assert(isinstance(newidadf, ibmdbpy.IdaDataFrame) is True)
            assert(len(newidadf.columns) == 2)
            assert(idadf.columns[0] == newidadf.columns[0])
            assert(idadf.columns[-1] == newidadf.columns[-1])


    def test_idadf_getitem_2_cols_idadf_keyerror(self, idadf):
        with pytest.raises(KeyError):
            idadf[[idadf.columns[0], "NOTEXISTING_COLUMN_455849820205"]]

    # TODO : FIX If you select twice the same columns, only one with be taken into account
    # (This is because they are referenced in a dictionary, maybe force modifying the name of the columns)

    def test_idadf_getitem_all_cols_idadf(self, idadf):
        if len(idadf.columns) >= 1:
            newidadf = idadf[list(idadf.columns)]
            assert(isinstance(newidadf, ibmdbpy.IdaDataFrame) is True)
            assert(len(newidadf.columns) == len(idadf.columns))
            assert(newidadf.shape == idadf.shape)

    def test_idadf_getitem_idaseries(self, idadf):
        if len(idadf.columns) >= 1:
            newidaseries = idadf[idadf.columns[0]]
            assert(isinstance(newidaseries, ibmdbpy.IdaSeries))
            assert(len(newidaseries.columns) == 1)
            assert(idadf.columns[0] == newidaseries.columns[0])

            newidaseries = idadf[idadf.columns[-1]]
            assert(isinstance(newidaseries, ibmdbpy.IdaDataFrame))
            assert(len(newidaseries.columns) == 1)
            assert(idadf.columns[-1] == newidaseries.columns[0])

    def test_idadf_getitem_idaseries_keyerror(self, idadf):
        with pytest.raises(KeyError):
            idadf["NOTEXISTING_COLUMN_455849820205"]

    def test_idadf_getitem_idaseries_keyerror_several_columns(self, idadf):
        if len(idadf.columns) >= 2:
            with pytest.raises(KeyError):
                idadf[idadf.columns[0], idadf.columns[1]]

    def test_idadf_getitem_slice(self, idadb, idadf, idadf_tmp):
        if len(idadf) > 10:
            newidadf = idadf[0:9]
            assert(len(newidadf) == 10)

        if len(idadf_tmp) > 10:
            idadb.add_column_id(idadf_tmp, destructive = True)
            newidadf_1 = idadf_tmp[0:9]
            newidadf_2 = idadf_tmp[0:9]
            assert(all(newidadf_1.head(10) == newidadf_2.head(10)))

    def test_idaseries_getitem_slice(self, idadb, idadf, idadf_tmp):
        # Set them as series first and do the same test as above
        if len(idadf.columns) >= 1:
            idadf = idadf[idadf.columns[0]]
            idadf_tmp = idadf_tmp[idadf_tmp.columns[0]]
            assert(isinstance(idadf, ibmdbpy.IdaDataFrame))
            assert(isinstance(idadf_tmp, ibmdbpy.IdaSeries))

            if len(idadf) > 10:
                newidadf = idadf[0:9]
                assert(len(newidadf) == 10)

    def test_idadf_setitem(self, idadf):
        pass

    def test_idadf_delitem(self, idadf):
        pass

    def test_idadf_filter_lt(self, idadf):
        pass

    def test_idadf_filter_le(self, idadf):
        pass

    def test_idadf_filter_eq(self, idadf):
        pass

    def test_idadf_filter_ne(self, idadf):
        pass

    def test_idadf_filter_ge(self, idadf):
        pass

    def test_idadf_filter_gt(self, idadf):
        pass

    def test_idadf_feature_add(self, idadf):
        pass

    def test_idadf_feature_radd(self, idadf):
        pass

    def test_idadf_feature_div(self, idadf):
        pass

    def test_idadf_feature_rdiv(self, idadf):
        pass

    def test_idadf_feature_floordiv(self, idadf):
        pass

    def test_idadf_feature_rfloordiv(self, idadf):
        pass

    def test_idadf_feature_mod(self, idadf):
        pass

    def test_idadf_feature_rmod(self, idadf):
        pass

    def test_idadf_feature_mul(self, idadf):
        pass

    def test_idadf_feature_rmul(self, idadf):
        pass

    def test_idadf_feature_neg(self, idadf):
        pass

    def test_idadf_feature_rpos(self, idadf):
        pass

    def test_idadf_feature_pow(self, idadf):
        pass

    def test_idadf_feature_rpow(self, idadf):
        pass

    def test_idadf_feature_sub(self, idadf):
        pass

    def test_idadf_feature_rsub(self, idadf):
        pass

class Test_DataBaseFeatures(object):

    def test_idadf_exists(self, idadf):
        assert(idadf.exists() is True)
        pass

    def test_idadf_is_view(self, idadf):
        assert(idadf.is_view() is False)
        pass

    def test_idadf_is_table(self, idadf):
        assert(idadf.exists() is True)
        pass

    def test_idadf_get_primary_key(self, idadf):
        pass

    def test_idadf_ida_query(self, idadf):
        pass

    def test_idadf_ida_scalar_query(self, idadf):
        pass


class Test_DataExploration(object):
    ### head
    # For head and tail we do not test if the rows match because
    # the order is not guaranteed anyway
    def test_idadf_head_default(self, idadb, idadf, df):
        sortkey = idadf.columns[0]
        if idadf._get_numerical_columns():
            sortkey = idadf._get_numerical_columns()[0]

        ida_head = idadf.head()
        assert isinstance(ida_head, pandas.core.frame.DataFrame)
        assert len(ida_head) == 5
        df_head = df.sort_values(sortkey).head()
        assert (ida_head[sortkey].tolist() == df_head[sortkey].tolist())

    def test_idadf_head_10(self, idadb, idadf, df):
        ida_head = idadf.head(10)
        assert isinstance(ida_head, pandas.core.frame.DataFrame)
        assert len(ida_head) == 10

    def test_idadf_head_10_sort(self, idadb, idadf, df):
        ida_head = idadf.head(10, sort=False)
        assert isinstance(ida_head, pandas.core.frame.DataFrame)
        assert len(ida_head) == 10

    def test_idadf_head_with_indexer(self, idadb, idadf_indexer, df):
        ida_head = idadf_indexer.head()
        sortby = len(df.columns)-1
        df_head = df.sort_values(df.columns[sortby]).head()
        assert isinstance(ida_head, pandas.core.frame.DataFrame)
        assert len(ida_head) == 5
        assert(ida_head[idadf_indexer.columns[sortby]].tolist() ==
                       df_head[df.columns[sortby]].tolist())

    def test_idadf_head_projected_3col(self, idadf, df):
        if len(idadf.columns) >= 4:
            columns = idadf.columns[1:4].tolist()
            newidadf = idadf[columns]

            sortkey = newidadf.columns[0]
            if newidadf._get_numerical_columns():
                sortkey = newidadf._get_numerical_columns()[0]

            ida_head = newidadf.head()

            df_sorted = df.sort_values(sortkey)
            df_head = df_sorted[columns].head()

            assert isinstance(ida_head, pandas.core.frame.DataFrame)
            assert len(ida_head) == 5
            assert(ida_head[sortkey].tolist() == df_head[sortkey].tolist())

    def test_idadf_head_sorted(self, idadf, df):
        sortIdx = len(df.columns) - 1
        sortkey = idadf.columns[sortIdx]
        newidadf = idadf.sort(sortkey)
        ida_head = newidadf.head()

        df_head = df.sort_values(sortkey).head()

        assert(" ORDER BY " in newidadf.internal_state.get_state())
        assert isinstance(ida_head, pandas.core.frame.DataFrame)
        assert len(ida_head) == 5
        assert(ida_head[sortkey].tolist() == df_head[sortkey].tolist())

    def test_idadf_head_0(self, idadf):
        with pytest.raises(ValueError):
            idadf.head(0)

    def test_idadf_head_negative(self, idadf):
        with pytest.raises(ValueError):
            idadf.head(-1)

    ### tail
    def test_idadf_tail_default(self, idadb, idadf, df):
        sortkey = idadf.columns[0]
        if idadf._get_numerical_columns():
            sortkey = idadf._get_numerical_columns()[0]
        ida_tail = idadf.tail()
        assert isinstance(ida_tail, pandas.core.frame.DataFrame)
        assert len(ida_tail) == 5
        df_tail = df.sort_values(sortkey).tail()
        assert (ida_tail[sortkey].tolist() == df_tail[sortkey].tolist())

    def test_idadf_tail_10(self, idadb, idadf, df):
        ida_tail = idadf.tail(10)
        assert isinstance(ida_tail, pandas.core.frame.DataFrame)
        assert len(ida_tail) == 10

    def test_idadf_tail_10_sort(self, idadb, idadf, df):
        ida_tail = idadf.tail(10, sort=False)
        assert isinstance(ida_tail, pandas.core.frame.DataFrame)
        assert len(ida_tail) == 10

    def test_idadf_tail_with_indexer(self, idadb, idadf_indexer, df):
        ida_tail = idadf_indexer.tail()
        sortby = len(df.columns)-1
        df_head = df.sort_values(df.columns[sortby]).tail()
        assert isinstance(ida_tail, pandas.core.frame.DataFrame)
        assert len(ida_tail) == 5
        assert(ida_tail[idadf_indexer.columns[sortby]].tolist() ==
                       df_head[df.columns[sortby]].tolist())

    def test_idadf_tail_projected_3col(self, idadf, df):
        if len(idadf.columns) >= 4:
            columns = idadf.columns[1:4].tolist()
            newidadf = idadf[columns]

            sortkey = newidadf.columns[0]
            if newidadf._get_numerical_columns():
                sortkey = newidadf._get_numerical_columns()[0]

            ida_tail = newidadf.tail()

            df_sorted = df.sort_values(sortkey)
            df_tail = df_sorted[columns].tail()

            assert isinstance(ida_tail, pandas.core.frame.DataFrame)
            assert len(ida_tail) == 5
            assert(ida_tail[sortkey].tolist() == df_tail[sortkey].tolist())

    @pytest.mark.skip(reason="tail on sorted dataframe fails in general, needs fixing first")
    def test_idadf_tail_sorted(self, idadf, df):
        sortIdx = len(df.columns) - 1
        sortkey = idadf.columns[sortIdx]
        newidadf = idadf.sort(sortkey)
        ida_tail = newidadf.tail()

        df_tail = df.sort_values(sortkey).tail()

        assert(" ORDER BY " in newidadf.internal_state.get_state())
        assert isinstance(ida_tail, pandas.core.frame.DataFrame)
        assert len(ida_tail) == 5
        assert(ida_tail[sortkey].tolist() == df_tail[sortkey].tolist())


    def test_idadf_tail_0(self, idadf):
        with pytest.raises(ValueError):
            idadf.tail(0)

    def test_idadf_tail_negative(self, idadf):
        with pytest.raises(ValueError):
            idadf.tail(-1)

    def test_idadf_pivot_table(self, idadf):
        pass

    def test_idadf_sort(self, idadf):
        pass

# no test
#__enter__
#__exit__
