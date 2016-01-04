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
Test module for private methods of IdaDataFrameObjects
"""
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from builtins import zip
from builtins import str
from future import standard_library
standard_library.install_aliases()

import pytest
import six

from ibmdbpy import IdaDataFrame
from ibmdbpy.exceptions import IdaDataBaseError


class Test_IdaDataBase_PrivateMethods(object):

    def test_idadb_upper_columns(self, idadb, df):
        df_upper = idadb._upper_columns(df)
        for column in df_upper.columns:
            assert(column == column.upper())

    def test_idadb_get_name_and_schema(self, idadb):
        tup = idadb._get_name_and_schema("SCHEMA.TABLE")
        assert(tup[0] == "SCHEMA")
        assert(tup[1] == "TABLE")

    def test_idadb_get_name_and_schema_no_schema(self, idadb):
        tup = idadb._get_name_and_schema("TABLE")
        assert(tup[0] == idadb.current_schema)
        assert(tup[1] == "TABLE")

    def test_idadb_get_valid_tablename_default(self, idadb):
        tablename = idadb._get_valid_tablename()
        assert(isinstance(tablename, six.string_types))
        assert("DATA_FRAME_" in tablename)

    def test_idadb_get_valid_tablename_custom(self, idadb):
        tablename = idadb._get_valid_tablename("MY_PERSONAL_PREFIX")
        assert("MY_PERSONAL_PREFIX" in tablename)

    def test_idadb_get_valid_tablename_error(self, idadb):
        with pytest.raises(ValueError):
            idadb._get_valid_tablename("INCORRECT_PREFIX_ß%$")

    def test_idadb_get_valid_viewname_default(self, idadb):
        viewname = idadb._get_valid_viewname()
        assert(isinstance(viewname, six.string_types))
        assert("VIEW_" in viewname)

    def test_idadb_get_valid_viewname_custom(self, idadb):
        viewname = idadb._get_valid_viewname("MY_PERSONAL_PREFIX")
        assert("MY_PERSONAL_PREFIX" in viewname)

    def test_idadb_get_valid_viewname_error(self, idadb):
        with pytest.raises(ValueError):
            idadb._get_valid_viewname("INCORRECT_PREFIX_ß%$")

    def test_idadb_get_valid_modelname_default(self, idadb):
        modelname = idadb._get_valid_modelname()
        assert(isinstance(modelname, six.string_types))
        assert("MODEL_" in modelname)

    def test_idadb_get_valid_modelname_custom(self, idadb):
        modelname = idadb._get_valid_modelname("MY_PERSONAL_PREFIX")
        assert("MY_PERSONAL_PREFIX" in modelname)

    def test_idadb_get_valid_modelname_error(self, idadb):
        with pytest.raises(ValueError):
            idadb._get_valid_modelname("INCORRECT_PREFIX_ß%$")

    def test_idadb_create_table(self, idadb, df):
        try : idadb.drop_table("CREATE_TABLE_TEST_585960708904")
        except : pass
        idadb._create_table(df, "CREATE_TABLE_TEST_585960708904")
        ida = IdaDataFrame(idadb, "CREATE_TABLE_TEST_585960708904")
        assert(all([str(x) == str(y) for x,y in zip(df.columns, ida.columns)]))
        assert(idadb.exists_table("CREATE_TABLE_TEST_585960708904") == 1)
        idadb.drop_table("CREATE_TABLE_TEST_585960708904")

    def test_idadb_create_view(self, idadb, df):
        try : idadb.drop_table("CREATE_VIEW_TEST_585960708904")
        except: pass
        idadb._create_table(df, "CREATE_VIEW_TEST_585960708904")
        ida = IdaDataFrame(idadb, "CREATE_VIEW_TEST_585960708904")
        try : idadb.drop_view("VIEW_TEST_585960708904")
        except: pass
        idadb._create_view(ida, "VIEW_TEST_585960708904")
        idadb.drop_table("CREATE_VIEW_TEST_585960708904")
        idadb.drop_view("VIEW_TEST_585960708904")

    # Make test when it fails too
    def test_idadb_insert_into_database(self, idadb, df):
        try : idadb.drop_table("INSERT_TEST_585960708904")
        except: pass
        idadb._create_table(df, "INSERT_TEST_585960708904")
        ida = IdaDataFrame(idadb, "INSERT_TEST_585960708904")
        assert(ida.shape[0] == 0)
        idadb._insert_into_database(df.loc[[1]], "INSERT_TEST_585960708904")
        del ida.shape
        assert(ida.shape[0] == 1)
        idadb.drop_table("INSERT_TEST_585960708904")

    def test_idadb_check_procedure(self, idadb):
        # This test work of course only if KMEANS is available
        bol = idadb._check_procedure('KMEANS')
        assert(bol is True)

    def test_idadb_check_procedure_fail(self, idadb):
        with pytest.raises(IdaDataBaseError):
            idadb._check_procedure('NOTEXISTINGPROCEDURE_495906823812')

    def test_idadb_reset_attributes(self, idadb):
        idadb.useless_attribute = "test"
        assert(hasattr(idadb, "useless_attribute"))
        idadb._reset_attributes("useless_attribute")
        assert(not hasattr(idadb, "useless_attribute"))
        idadb.useless_attribute = "test"
        assert(hasattr(idadb, "useless_attribute"))
        idadb._reset_attributes(["useless_attribute"])
        assert(not hasattr(idadb, "useless_attribute"))

    def test_idadb_reset_attributes_not_existing(self, idadb):
        assert(not hasattr(idadb, "NOTEXISTINGATTRIBUTE_374965038285"))
        idadb._reset_attributes("NOTEXISTINGATTRIBUTE_374965038285")
        assert(not hasattr(idadb, "NOTEXISTINGATTRIBUTE_374965038285"))

    def test_idadb_reset_attributes_multiple(self, idadb):
        idadb.useless_attribute1 = "test"
        idadb.useless_attribute2 = "test"
        assert(hasattr(idadb, "useless_attribute1"))
        assert(hasattr(idadb, "useless_attribute2"))
        assert(not hasattr(idadb, "useless_attribute3"))
        idadb._reset_attributes(["useless_attribute1", "useless_attribute2", "useless_attribute3"])
        # Note : One is actually not existing
        assert(not hasattr(idadb, "useless_attribute1"))
        assert(not hasattr(idadb, "useless_attribute2"))
        assert(not hasattr(idadb, "useless_attribute3"))

    def test_idadb_reset_retrieve_cache(self, idadb):
        idadb.my_custom_cache = "test"
        cache = idadb._retrieve_cache("my_custom_cache")
        assert(cache == idadb.my_custom_cache)

    def test_idadb_reset_retrieve_cache_not_existing(self, idadb):
        cache = idadb._retrieve_cache("NOTEXISTINGCACHE_43845696794939")
        assert(cache is None)