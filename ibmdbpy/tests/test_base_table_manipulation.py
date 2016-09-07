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
from future import standard_library
standard_library.install_aliases()

import pytest

from ibmdbpy import IdaDataFrame
from ibmdbpy.learn import KMeans

class Test_DeleteDataBaseObjects(object):

    def test_idadb_drop_table(self, idadb, idadf_tmp):
        assert(idadb.exists_table(idadf_tmp.name) == 1)
        idadb.drop_table(idadf_tmp.name)
        assert(idadb.exists_table(idadf_tmp.name) == 0)

    def test_idadb_drop_table_value_error(self, idadb):
        with pytest.raises(ValueError):
            idadb.drop_table("NOTEXISTINGOBJECT_496070383095079384063739509")

    def test_idadb_drop_table_type_error(self, idadb, idaview):
        with pytest.raises(TypeError):
            idadb.drop_table(idaview.name) # this is a view

    def test_idadb_drop_view(self, idadb, idaview_tmp):
        assert(idadb.exists_view(idaview_tmp.name) == 1)
        idadb.drop_view(idaview_tmp.name)
        assert(idadb.exists_view(idaview_tmp.name) == 0)

    def test_idadb_drop_view_value_error(self, idadb):
        with pytest.raises(ValueError):
            idadb.drop_view("NOTEXISTINGOBJECT_496070383095079384063739509")

    def test_idadb_drop_view_type_error(self, idadb, idadf):
        with pytest.raises(TypeError):
            idadb.drop_view(idadf.name) # this is a table

    def test_idadb_drop_model_positive(self, idadb, idadf_tmp):
        idadb.add_column_id(idadf_tmp, destructive = True)
        # Create a simple KMEANS model
        kmeans = KMeans(n_clusters = 3)
        kmeans.fit(idadf_tmp)
        assert(idadb.is_model(kmeans.modelname) == 1)
        idadb.drop_model(kmeans.modelname)
        idadb.commit()

    def test_idadb_drop_model_value_error(self, idadb):
        with pytest.raises(ValueError):
            idadb.is_model("NOTEXISTINGOBJECT_496070383095079384063739509")

    def test_idadb_drop_model_type_error(self, idadb, idadf, idaview):
        with pytest.raises(TypeError):
            idadb.drop_model(idadf.name)
        with pytest.raises(TypeError):
            idadb.drop_model(idaview.name)
        #with pytest.raises(TypeError):
        #    idadb.drop_model("ST_INFORMTN_SCHEMA.ST_UNITS_OF_MEASURE")

class Test_TableManipulation(object):
    def test_idadb_rename(self, idadb, idadf_tmp):
        try:
            idadb.drop_table("TEST_RENAMED")
        except:
            pass
        original_name = idadf_tmp.tablename
        idadb.rename(idadf_tmp, "TEST_RENAMED")
        idadb.commit()
        assert(idadf_tmp.name == "TEST_RENAMED")
        assert(idadb.exists_table("TEST_RENAMED") == 1)
        assert(idadb.exists_table(original_name) == 0)
        idadb.rename(idadf_tmp, original_name)

    def test_idadb_rename_value_error(self, idadb, idadf):
        with pytest.raises(ValueError):
            idadb.rename(idadf, "T569ß4359**4\4%")

    def test_idadb_rename_type_error(self, idadb, idaview):
        with pytest.raises(TypeError):
            idadb.rename(idaview, "TEST_VIEW_RENAME")

    def test_idadb_rename_name_error(self, idadb, idadf_tmp):
        with pytest.raises(NameError):
            idadb.rename(idadf_tmp, idadf_tmp.tablename)

    def test_idadb_add_column_id_destructive(self, idadb, idadf_tmp):
        idadb.add_column_id(idadf_tmp, "ID", destructive = True)
        assert("ID" in idadf_tmp.columns)
        assert("ID" == idadf_tmp.indexer)
        assert("ID" in idadf_tmp._get_all_columns_in_table())
        idadf_tmp_new = IdaDataFrame(idadb, idadf_tmp._name, indexer = "ID")
        assert("ID" in idadf_tmp_new.columns)
        assert("ID" == idadf_tmp_new.indexer)
        assert("ID" in idadf_tmp_new._get_all_columns_in_table())

    def test_idadb_add_column_id_non_destructive(self, idadb, idadf_tmp):
        idadb.add_column_id(idadf_tmp, "ID", destructive = False)
        assert("ID" in idadf_tmp.columns)
        assert("ID" == idadf_tmp.indexer)
        assert("ID" not in idadf_tmp._get_all_columns_in_table())
        idadf_tmp_new = IdaDataFrame(idadb, idadf_tmp._name)
        assert("ID" not in idadf_tmp_new.columns)
        assert("ID" not in idadf_tmp_new._get_all_columns_in_table())

    def test_idadb_add_column_id_non_destructive_custom_name(self, idadb, idadf_tmp):
        idadb.add_column_id(idadf_tmp, "MY_CUSTOM_ID", destructive = False)
        assert("MY_CUSTOM_ID" in idadf_tmp.columns)
        assert("MY_CUSTOM_ID" == idadf_tmp.indexer)
        assert("MY_CUSTOM_ID" not in idadf_tmp._get_all_columns_in_table())
        idadf_tmp_new = IdaDataFrame(idadb, idadf_tmp._name)
        assert("MY_CUSTOM_ID" not in idadf_tmp_new.columns)
        assert("MY_CUSTOM_ID" not in idadf_tmp_new._get_all_columns_in_table())

    def test_idadb_add_column_id_non_destructive_value_error(self, idadb, idadf_tmp):
        with pytest.raises(ValueError):
            idadb.add_column_id(idadf_tmp, idadf_tmp.columns[0], destructive = False)

    def test_idadb_add_column_id_non_destructive_type_error(self, idadb, df):
        with pytest.raises(TypeError):
            idadb.add_column_id(df, "ID", destructive = False)

    def test_idadb_delete_column_value_error(self, idadb, idadf):
        with pytest.raises(ValueError):
            idadb.delete_column(idadf, "NOTEXISTING_COLUMN_309599439")

    def test_idadb_delete_column_type_error(self, idadb, idadf):
        with pytest.raises(TypeError):
            idadb.delete_column(idadf, idadf)

    def test_idadb_delete_column_destructive(self, idadb, idadf_tmp):
        to_delete = idadf_tmp.columns[0]
        idadb.delete_column(idadf_tmp, to_delete, destructive = True)
        assert(to_delete not in idadf_tmp.columns)
        assert(to_delete not in idadf_tmp._get_all_columns_in_table())
        idadf_tmp_new = IdaDataFrame(idadb, idadf_tmp._name)
        assert(to_delete not in idadf_tmp_new.columns)
        assert(to_delete not in idadf_tmp_new._get_all_columns_in_table())

    def test_idadb_delete_column_destructive_real_indexer(self, idadb, idadf_tmp):
        idadb.add_column_id(idadf_tmp, "ID", destructive = True)
        idadb.delete_column(idadf_tmp, "ID", destructive = True)
        assert("ID" not in idadf_tmp.columns)
        assert("ID" != idadf_tmp.indexer)
        assert("ID" not in idadf_tmp._get_all_columns_in_table())
        idadf_tmp_new = IdaDataFrame(idadb, idadf_tmp._name)
        assert("ID" not in idadf_tmp_new.columns)
        assert("ID" not in idadf_tmp_new._get_all_columns_in_table())

    def test_idadb_delete_column_destructive_virtual_indexer(self, idadb, idadf_tmp):
        idadb.add_column_id(idadf_tmp, "ID", destructive = False)
        idadb.delete_column(idadf_tmp, "ID", destructive = True)
        assert("ID" not in idadf_tmp.columns)
        assert("ID" != idadf_tmp.indexer)
        assert("ID" not in idadf_tmp._get_all_columns_in_table())
        idadf_tmp_new = IdaDataFrame(idadb, idadf_tmp._name)
        assert("ID" not in idadf_tmp_new.columns)
        assert("ID" not in idadf_tmp_new._get_all_columns_in_table())

    def test_idadb_delete_column_non_destructive(self, idadb, idadf_tmp):
        to_delete = idadf_tmp.columns[0]
        idadb.delete_column(idadf_tmp, to_delete, destructive = False)
        assert(to_delete not in idadf_tmp.columns)
        assert(to_delete in idadf_tmp._get_all_columns_in_table())
        idadf_tmp_new = IdaDataFrame(idadb, idadf_tmp._name)
        assert(to_delete in idadf_tmp_new.columns)
        assert(to_delete in idadf_tmp_new._get_all_columns_in_table())

    def test_idadb_delete_column_non_destructive_real_indexer(self, idadb, idadf_tmp):
        idadb.add_column_id(idadf_tmp, "ID", destructive = True)
        idadb.delete_column(idadf_tmp, "ID", destructive = False)
        assert("ID" not in idadf_tmp.columns)
        assert("ID" != idadf_tmp.indexer)
        assert("ID" in idadf_tmp._get_all_columns_in_table())
        idadf_tmp_new = IdaDataFrame(idadb, idadf_tmp._name, indexer = "ID")
        assert("ID" in idadf_tmp_new.columns)
        assert("ID" == idadf_tmp_new.indexer)
        assert("ID" in idadf_tmp_new._get_all_columns_in_table())

    def test_idadb_delete_column_non_destructive_virtual_indexer(self, idadb, idadf_tmp):
        idadb.add_column_id(idadf_tmp, "ID", destructive = False)
        idadb.delete_column(idadf_tmp, "ID", destructive = False)
        assert("ID" not in idadf_tmp.columns)
        assert("ID" != idadf_tmp.indexer)
        assert("ID" not in idadf_tmp._get_all_columns_in_table())
        idadf_tmp_new = IdaDataFrame(idadb, idadf_tmp._name)
        assert("ID" not in idadf_tmp_new.columns)
        assert("ID" not in idadf_tmp_new._get_all_columns_in_table())

    def test_idadb_append(self, idadb, idadf_tmp, df):
        nrow = idadf_tmp.shape[0]
        ncol = idadf_tmp.shape[1]
        idadb.append(idadf_tmp, df)
        assert(idadf_tmp.shape[0] == nrow*2)
        nrow = idadf_tmp.shape[0]
        idadb.append(idadf_tmp, df.loc[0:4])
        assert(idadf_tmp.shape[0] == nrow + 5)
        assert(idadf_tmp.shape[1] == ncol)