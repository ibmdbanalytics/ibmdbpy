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

from numbers import Number

import pandas
import pytest
import six

from ibmdbpy import IdaDataBase
from ibmdbpy.exceptions import IdaDataBaseError

class Test_ConnectToDB(object):

    def test_idadb_attr(self, idadb):
        assert isinstance(idadb.data_source_name, six.string_types)
        assert isinstance(idadb._con_type, six.string_types)
        assert isinstance(idadb._idadfs, list)
        if idadb._con_type == 'odbc':
            assert isinstance(idadb._connection_string, six.string_types)
        elif idadb._con_type == 'jdbc':
            assert isinstance(idadb._connection_string, list)
        assert(type(idadb._con).__name__ in ['Connection','instance'])
        assert(idadb._con_type in ["odbc", "jdbc"])

    def test_idadb_instance_fail(self, idadb):
        with pytest.raises(IdaDataBaseError):
            if idadb._con_type == 'odbc':
                IdaDataBase(dsn = 'NOTEXISTING_DATASOURCE')
            else:
                IdaDataBase(dsn='jdbc:NOTEXISTING_DATASOURCE')
        with pytest.raises(IdaDataBaseError):
            IdaDataBase(dsn = 'jdbc:db2://awh-yp-small03.services.dal.bluemix.net:50000/BLUDB:user=XXXXXXXXXX;password=XXXXXXXXXXXX',
                        uid="hello", pwd="world")
        with pytest.raises(IdaDataBaseError):
            IdaDataBase(dsn = 'jdbc:db2://awh-yp-small03.services.dal.bluemix.net:50000/BLUDB')


class Test_ConnexionManagement(object):

    def test_idadb_commit(self, idadb, df):
        try : idadb.drop_table("TEST_COMMIT_5869703824607040")
        except : pass
        idadb._create_table(df, "TEST_COMMIT_5869703824607040")
        idadb.commit()
        #idadb.rback()
        assert(idadb.exists_table("TEST_COMMIT_5869703824607040") == 1)
        idadb.drop_table("TEST_COMMIT_5869703824607040")
        idadb.commit()

    def test_idadb_rollback(self, idadb, df):
        # TODO: Does not work for JDBC -> Why no rollback for jaydebeapi ?
        if idadb._con_type == 'odbc':
            try:
                idadb.drop_table("TEST_ROLLBACK_59673030586849305074")
            except:
                pass
            idadb.commit()
            idadb._create_table(df, "TEST_ROLLBACK_59673030586849305074")
            idadb.rollback()
            idadb.commit()
            assert(idadb.exists_table("TEST_ROLLBACK_59673030586849305074") == 0)

    def test_idadb_close(self, idadb_tmp):
        idadb_tmp.close()
        with pytest.raises(IdaDataBaseError):
            idadb_tmp.show_tables()

    def test_reconnect(self, idadb_tmp):
        idadb_tmp.close()
        with pytest.raises(IdaDataBaseError):
            idadb_tmp.show_tables()
        idadb_tmp.reconnect()
        assert(isinstance(idadb_tmp.show_tables(), pandas.DataFrame))


class Test_UploadDataFrame(object):

    def test_idadb_as_idadataframe(self, idadb, df):
        ida = idadb.as_idadataframe(df, "TEST_AS_IDADF_18729493954_23849590", clear_existing = True)
        assert(all(ida.columns == df.columns))
        assert(list(ida.index) == list(df.index))
        assert(ida.shape == df.shape)
        idadb.drop_table("TEST_AS_IDADF_18729493954_23849590")

    def test_idadb_ida_query(self, idadb, idadf):
        query = "SELECT * FROM %s FETCH FIRST 5 ROWS ONLY"%idadf.name
        df = idadb.ida_query(query)
        assert(isinstance(df,pandas.DataFrame))
        assert(len(idadf.columns) == len(df.columns))
        assert(len(df) == 5)

    def test_idadb_ida_query_first_row_only(self, idadb, idadf, df):
        query = "SELECT * FROM %s FETCH FIRST 5 ROWS ONLY"%idadf.name
        downloaded_df = idadb.ida_query(query, first_row_only=True)
        assert(isinstance(downloaded_df,tuple))
        assert(len(downloaded_df) == len(df.loc[0]))

    def test_idadb_ida_scalar_query(self, idadb, idadf):
        query = "SELECT * FROM %s FETCH FIRST 5 ROWS ONLY"%idadf.name
        downloaded_df = idadb.ida_scalar_query(query)
        assert(isinstance(downloaded_df,six.string_types)|isinstance(downloaded_df,Number))

    ################# MORE TEST TO DO HERE
