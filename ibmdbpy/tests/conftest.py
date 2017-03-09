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
Configuration for pytest testing framework module
=================================================

To launch the test routine, do in ibmdbpy-master folder: py.test
Per default, py.test routine assumes a data source "DASHDB" is defined in ODBC
settings, with userID and password, and test with the well-known 'iris' dataset.
This can be changed using command line options, defined as following :

Options
-------
dsn
    Data source name to use for testing

uid
    UserID credential to connect to the data source

pwd
    Password credential to connect to the data source

jdbc
    JDBC url string to be used to connect to the data source, should comply
    to this template : jdbc:db2://<HOST>:<PORT>/<DBNAME>:user=<UID>;password=<PWD>

table
    Dataset to use for testing

Examples
--------
py.test
    Launch py.test routine using iris sampledataset in "DASHDB" ODBC data source.

py.test --dsn=<DSN>
    Do the test routine with the data source <DSN> as defined in ODBC settings.

py.test --dsn=<DSN> --uid=<UID> --pwd=<pwd>
    In case userID and password are not stored in ODBC settings.

py.test --jdbc=<jdbc_url_string>
    Do the test routine using jdbc connection

py.test --table=<DATA>
    Do the test routine using the dataset <DATA>


Notes
-----
Several sample datasets are provided for testing : titanic, iris, swiss
All options can be combined, however:
    * 'uid' and 'pwd' cannot be defined if 'jdbc' is defined.
    * 'uid' and 'password' must both be defined if one of them is already given.

"""

# Python 2 compatibility
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from builtins import str
from future import standard_library
standard_library.install_aliases()

import pytest
import ibmdbpy

def pytest_addoption(parser):
    """
    Definition of admissible options for the pytest command line
    """
    parser.addoption("--table", default="iris",
        help="Name of the table to test the dataset")
    parser.addoption("--dsn", default="DASHDB",
        help="Data Source Name")
    parser.addoption("--uid", default='',
        help="User ID")
    parser.addoption("--pwd", default='',
        help="Password")
    parser.addoption("--jdbc", default='',
        help="jdbc url string for JDBC connection")

def get_data(request):
    """
    Helper function that returns the dataset correponding to the table option
    (default, iris). Raise an error in case the dataset is not included in
    ibmdbpy or its name is unkown.
    """
    table = request.config.getoption('--table')
    if table == "titanic":
        from ibmdbpy.sampledata.titanic import titanic
        return titanic
    elif table == "iris":
        from ibmdbpy.sampledata.iris import iris
        return iris
    elif table == "swiss":
        from ibmdbpy.sampledata.swiss import swiss
        return swiss
    else:
        raise ValueError("Unknown table " + str(table))

# Fixture definitions

@pytest.fixture(scope="session")
def idadb(request):
    """
    DataBase connection fixture, to be used for the whole testing session.
    Hold the main IdaDataBase object. Shall not be closed except by a
    pytest finalizer.
    """
    def fin():
        try:
            idadb.close()
        except:
            pass
    request.addfinalizer(fin)

    jdbc = request.config.getoption('--jdbc')
    if jdbc != '':
        try:
            idadb = ibmdbpy.IdaDataBase(dsn=jdbc, autocommit=False)
        except:
            raise
    else:
        try:
            idadb = ibmdbpy.IdaDataBase(dsn=request.config.getoption('--dsn'),
                                uid=request.config.getoption('--uid'),
                                pwd=request.config.getoption('--pwd'),
                                autocommit=False)
        except:
            raise
    return idadb

@pytest.fixture(scope="function")
def idadb_tmp(request):
    """
    DataBase connection fixture, to be used by destructive and
    semi-destructive functions.
    """
    def fin():
        try:
            idadb_tmp.close()
        except:
            pass
    request.addfinalizer(fin)

    jdbc = request.config.getoption('--jdbc')
    if jdbc != '':
        try:
            idadb_tmp = ibmdbpy.IdaDataBase(dsn=jdbc, autocommit=False)
        except:
            raise
    else:
        try:
            idadb_tmp = ibmdbpy.IdaDataBase(dsn=request.config.getoption('--dsn'),
                                uid=request.config.getoption('--uid'),
                                pwd=request.config.getoption('--pwd'),
                                autocommit=False)
        except:
            raise
    return idadb_tmp

@pytest.fixture(scope="session")
def idadf(request, idadb):
    """
    IdaDataFrame fixture to be used for the whole testing session.
    Shall not be used during the testing procedure by destructive and
    semi-destructive functions.
    """
    data = get_data(request)

    def fin():
        try:
            idadb.drop_table("TEST_IBMDBPY")
            idadb.commit()
        except:
            pass
    request.addfinalizer(fin)

    idadf = idadb.as_idadataframe(data, 'TEST_IBMDBPY', clear_existing = True)
    idadb.commit()
    return idadf

@pytest.fixture(scope="session")
def idadf_indexer(request, idadb, idadf):
    """
    IdaDataFrame fixture with indexer defined to be used for the whole testing session.
    Shall not be used during the testing procedure by destructive and
    semi-destructive functions.
    """
    data = get_data(request)

    def fin():
        try:
            idadb.drop_table("TEST_IBMDBPY_INDEX")
            idadb.commit()
        except:
            pass
    request.addfinalizer(fin)

    idadf_index = idadb.as_idadataframe(data, 'TEST_IBMDBPY_INDEX',
                                        clear_existing=True,
                                        indexer=idadf.columns[len(idadf.columns) - 1])

    idadb.commit()
    return idadf_index

@pytest.fixture(scope="function")
def idadf_tmp(request, idadb):
    """
    IdaDataFrame fixture to be used by destructive and semi-destructive
    functions. To be considered as a temporary DataFrame that is created
    and destroyed for each function that requires it.
    """
    data = get_data(request)

    def fin():
        try:
            idadb.drop_table("TEST_IBMDBPY_TMP")
            idadb.commit()
        except:
            pass
    request.addfinalizer(fin)

    idadf = idadb.as_idadataframe(data, 'TEST_IBMDBPY_TMP', clear_existing = True)
    idadb.commit()
    return idadf
    
@pytest.fixture(scope="function")
def idageodf_county(idadb):
    """
    IdaGeoDataFrame to test geospatial methods
    This refers to the table 'GEO_COUNTY' of the 'SAMPLES' schema
    The table has one geometry column named 'SHAPE' of type 'ST_MULTIPOLYGON'
    Don't use it for destructive nor non-destructive methods (modify columns)
    """
    idageodf = ibmdbpy.IdaGeoDataFrame(idadb, 'SAMPLES.GEO_COUNTY', indexer='OBJECTID')
    return idageodf

@pytest.fixture(scope="function")
def idageodf_customer(idadb):
    """
    IdaGeoDataFrame to test geospatial methods
    This refers to the table 'GEO_CUSTOMER' of the 'SAMPLES' schema
    The table has one geometry column named 'SHAPE' of type 'ST_POINT'
    Don't use it for destructive nor non-destructive methods (modify columns)
    """
    idageodf = ibmdbpy.IdaGeoDataFrame(idadb, 'SAMPLES.GEO_CUSTOMER', indexer='OBJECTID')
    return idageodf    
    
@pytest.fixture(scope="function")
def idageodf_tornado(idadb):
    """
    IdaGeoDataFrame to test geospatial methods
    This refers to the table 'GEO_TORNADO' of the 'SAMPLES' schema
    The table has one geometry column named 'SHAPE' of type 'ST_MULTILINESTRING'
    Don't use it for destructive nor non-destructive methods (modify columns)
    """
    idageodf = ibmdbpy.IdaGeoDataFrame(idadb, 'SAMPLES.GEO_TORNADO')
    return idageodf

@pytest.fixture(scope="session")
def idaview(request, idadb, idadf):
    """
    IdaDataFrame fixture to be used for the whole testing session. Open a view
    based on idadf fixture.
    """
    def fin():
        try:
            idadb.drop_view("TEST_VIEW_ibmdbpy")
            idadb.commit()
        except:
            pass
    request.addfinalizer(fin)

    if idadb.exists_view("TEST_VIEW_ibmdbpy"):
        idadb.drop_view("TEST_VIEW_ibmdbpy")

    idadb._create_view(idadf, "TEST_VIEW_ibmdbpy")
    return ibmdbpy.IdaDataFrame(idadb, "TEST_VIEW_ibmdbpy")

@pytest.fixture(scope="function")
def idaview_tmp(request, idadb, idadf):
    """
    IdaDataFrame fixture to be used by destructive and semi-destructive
    functions. To be considered as a temporary DataFrame that is created
    and destroyed for each function that requires it. Opens a view based on
    idadf fixture.
    """
    def fin():
        try:
            idadb.drop_view("TEST_VIEW_ibmdbpy_TMP")
            idadb.commit()
        except:
            pass
    request.addfinalizer(fin)

    if idadb.exists_view("TEST_VIEW_ibmdbpy_TMP"):
        idadb.drop_view("TEST_VIEW_ibmdbpy_TMP")

    idadb._create_view(idadf, "TEST_VIEW_ibmdbpy_TMP")
    return ibmdbpy.IdaDataFrame(idadb, "TEST_VIEW_ibmdbpy_TMP")

@pytest.fixture(scope="session")
def df(request):
    """
    Original physical dataset to be used to testing. To be used by
    non-destructive function on the whole testing procedure.
    """
    return get_data(request)

@pytest.fixture(scope = "session", autouse=True)
def session_setup(idadb, request):
    """
    Defines actions to be done before starting the testing procedure.
    """
    # empty
    return

@pytest.fixture(scope = "session", autouse=True)
def session_teardown(idadb, idadf, idaview, request):
    """
    Defines cleanup actions to be done once the testing procedure is done.
    """
    def fin():
        try:
            idadb.drop_table(idadf.name)
            idadb.drop_view(idaview.name)
            idadb.commit()
            idadb.close()
        except: pass
    request.addfinalizer(fin)
    return

#def pytest_generate_tests(metafunc):
#    if 'table' in metafunc.fixturenames:
#        metafunc.parametrize("table",
#                             metafunc.config.option.table, scope = 'session')

