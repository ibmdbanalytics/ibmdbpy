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

# For local installation do "pip install -e" in the ibmdbpy-master directory
###############################################################################

"""
An IdaDataBase instance represents a reference to a remote dashDB/DB2 instance,
maintaining attributes and methods for administration of the database.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from builtins import zip
from builtins import int
from builtins import str
from future import standard_library
standard_library.install_aliases()

import os
#from os import path
import sys
import math
import random
from time import time
import datetime
import warnings
from copy import deepcopy

from collections import OrderedDict

import numpy as np
import pandas as pd

from lazy import lazy
import six

import ibmdbpy
from ibmdbpy import sql
from ibmdbpy.utils import timed, set_verbose, set_autocommit
from ibmdbpy.exceptions import IdaDataBaseError, PrimaryKeyError

class IdaDataBase(object):
    """
    An IdaDataBase instance represents a reference to a remote dashDB/DB2 
    instance. This is an abstraction layer for the remote connection. The 
    IdaDataBase interface provides several functions that enable basic database 
    administration in pythonic syntax.

    You can use either ODBC or JDBC to connect to the database. The default 
    connection type is ODBC, which is the standard connection type for Windows 
    users. To establish an ODBC connection, download an IBM DB2 driver and set 
    up your ODBC connection by specifying your connection protocol, port, and 
    hostname. An ODBC connection on Linux or Mac might require more settings. 
    For more information about how to establish an ODBC connection, see the 
    pypyodbc documentation.

    To connect with JDBC, install the optional external package jaydebeapi, 
    download the ibm jdbc driver, and save it in your local ibmdbpy folder. 
    If you put the jdbc driver in the CLASSPATH variable or the folder that
    contains it, it will work too. A C++ compiler adapted to the current python 
    version, operating system, and architecture may also be required to install
    jaydebeapi.

    The instantiation of an IdaDataBase object is a mandatory step before 
    creating IdaDataFrame objects because IdaDataFrames require an IdaDataBase 
    as a parameter to be initialized. By convention, use only one instance of 
    IdaDataBase per database. However, you can use several instances of 
    IdaDataFrame per connection.
    """

    def __init__(self, dsn, uid='', pwd='', autocommit=True, verbose=False):
        """
        Open a database connection.

        Parameters
        ----------
        dsn : str
            Data Source Name (as specified in your ODBC settings) or JDBC URL 
            string.

        uid : str, optional
            User ID.

        pwd : str, optional
            User password.

        autocommit : bool, default: True
            If True, automatically commits all operations.

        verbose : bool, defaukt: True
            If True, prints all SQL requests that are sent to the database. 

        Attributes
        ----------
        data_source_name : str
            Name of the referring DataBase.

        _con_type : str
            Type of the connection, either 'odbc' or 'jdbc'.

        _connection_string : str
            Connection string use for connecting via ODBC or JDBC.

        _con : connection object
            Connection object to the remote Database.

        _idadfs : list
            List of IdaDataFrame objects opened under this connection.

        Returns
        -------
        IdaDataBase object

        Raises
        ------
        ImportError
            JayDeBeApi is not installed.
        IdaDataBaseError
            * uid and pwd are defined both in uid, pwd parameters and dsn.
            * The 'db2jcc4.jar' file is not in the ibmdbpy site-package repository.

        Examples
        --------

        ODBC connection, userID and password are stored in ODBC settings:

        >>> IdaDataBase(dsn="DASHDB") # ODBC Connection
        <ibmdbpy.base.IdaDataBase at 0x9bec860>

        ODBC connection, userID and password are not stored in ODBC settings:

        >>> IdaDataBase(dsn="DASHDB", uid="<UID>", pwd="<PWD>")
        <ibmdbpy.base.IdaDataBase at 0x9bec860>
        
        JDBC connection, full JDBC string:

        >>> jdbc='jdbc:db2://<HOST>:<PORT>/<DBNAME>:user=<UID>;password=<PWD>'
        >>> IdaDataBase(dsn=jdbc)
        <ibmdbpy.base.IdaDataBase at 0x9bec860>

        JDBC connectiom, JDBC string and seperate userID and password:

        >>> jdbc = 'jdbc:db2://<HOST>:<PORT>/<DBNAME>'
        >>> IdaDataBase(dsn=jdbc, uid="<UID>", pwd="<PWD>")
        <ibmdbpy.base.IdaDataBase at 0x9bec860>
        """
        for arg,name in zip([dsn, uid, pwd],['dsn','uid','pwd']):
            if not isinstance(arg, six.string_types):
                raise TypeError("Argument '%s' of type %, expected : string type."%(name,type(arg)))

        self.data_source_name = dsn

        # Detect if user attempt to connection with ODBC or JDBC
        if "jdbc:" in dsn:
            self._con_type = "jdbc"
        else:
            self._con_type = "odbc"

        self._idadfs = []

        if self._con_type == 'odbc':
            self._connection_string = "DSN=%s; UID=%s; PWD=%s;LONGDATACOMPAT=1;"%(dsn,uid,pwd)
            """
            Workaround for CLOB retrieval: 
            Set the CLI/ODBC LongDataCompat keyword to 1. 
            Doing so will force the CLI driver to make the 
            following data type mappings
            SQL_CLOB to SQL_LONGVARCHAR
            SQL_BLOB to SQL_LONGVARBINARY
            SQL_DBCLOB to SQL_WLONGVARCHAR
            """
            import pypyodbc
            try :
                self._con = pypyodbc.connect(self._connection_string)
            except Exception as e:
                raise IdaDataBaseError(e.value[1])

        if self._con_type == 'jdbc':
            # Sanity check
            if ('user=' in dsn)&('password=' in dsn):
                if (uid != '') | (pwd != ''):
                    message = ("Ambiguous definition of userID or password: " +
                               "Cannot be defined in  uid and pwd parameters "+
                               "and in jdbc_url_string at the same time.")
                    raise IdaDataBaseError(message)

                # Parsing the jdbc url string
                jdbc_url = dsn.split(":user=")[0]
                arguments = dsn.split(":")[-1].split(';')
                uid = arguments[0].split('user=')[-1]
                pwd = arguments[1].split('password=')[-1]
            else:
                if (uid == '') | (pwd == ''):
                    message = ("Missing credentials to connect via JDBC.")
                    raise IdaDataBaseError(message)
                jdbc_url = dsn

            try:
                import jaydebeapi
                import jpype
            except ImportError:
                raise ImportError("Please install optional dependency jaydebeapi "+
                            "to work with JDBC.")
            
            here = os.path.abspath(os.path.dirname(__file__))
            
            if not jpype.isJVMStarted():
                classpath = os.getenv('CLASSPATH','')
                jarpath = ''
                platform = sys.platform
                
                if verbose: print("Trying to find a path to the db2 jcc driver in CLASSPATH...")
                if ("db2jcc4.jar" in classpath) or ("db2jcc.jar" in classpath):
                    # A path to the driver exists in the classpath variable
                    if platform == 'win32':
                        db2jcclist = [x for x in classpath.split(';') if (("db2jcc4.jar" in x) or ("db2jcc.jar" in x))]                        
                    else:
                        db2jcclist = [x for x in classpath.split(':') if (("db2jcc4.jar" in x) or ("db2jcc.jar" in x))]
                    
                    jarpath = db2jcclist.pop(0) # just take the first
                    
                    if not os.path.isfile(jarpath): # Is the path correct?
                        if verbose: 
                            print("The path %s does not seem to be correct. Trying to recover..."%jarpath)
                        jarpath = ''
                        while db2jcclist: # Try others
                            jarpath = db2jcclist.pop(0)
                            if not os.path.isfile(jarpath): 
                                if verbose: 
                                    print("The path %s does not seem to be correct. Trying to recover..."%jarpath)
                                jarpath = ''
                                
                    if jarpath and (platform == 'win32'):
                        jarpath = jarpath.split(':')[1].replace('\\', '/') # get rid of windows style formatting
                            
                if not jarpath: # Means the search for a direct path in the CLASSPATH was not successful
                    def _get_db2driver_from_folder(folder):
                        jarpath = ''
                        if platform == 'win32':
                            # Windows specific code
                            if os.path.isfile(folder + "\\db2jcc4.jar"):
                                jarpath = folder.split(':')[1].replace('\\', '/') + "/db2jcc4.jar"
                            elif os.path.isfile(folder + "\\db2jcc.jar"):
                                jarpath = folder.split(':')[1].replace('\\', '/') + "/db2jcc.jar"
                        else:
                            if os.path.isfile(folder + "/db2jcc4.jar"):
                                jarpath = folder + "/db2jcc4.jar"
                            elif os.path.isfile(folder + "/db2jcc.jar"):
                                jarpath = folder + "/db2jcc.jar"
                        return jarpath
                        
                    if classpath: # There is at least something in the classpath variable
                        # Let us see if the jar in a folder of the classpath
                        if verbose: print("Trying to find the db2 jcc driver in the folders of CLASSPATH...")
                        if sys.platform == 'win32':
                            db2jcclist = [x for x in classpath.split(';')]
                        else:
                            db2jcclist = [x for x in classpath.split(':')]
                        for folder in db2jcclist:
                            if not jarpath:
                                jarpath = _get_db2driver_from_folder(folder)
                    
                    if not jarpath: # jarpath is still ''
                        if verbose: print("Trying to find the db2 jcc driver in the local folder of ibmdbpy (%s)"%here)
                        # Try to get the path to the driver from the ibmdbpy folder, the last chance
                        jarpath = _get_db2driver_from_folder(here)
                    
                if jarpath:
                    if verbose: print("Found it at %s! Trying to connect..."%jarpath)
                       
                jpype.startJVM(jpype.getDefaultJVMPath(), '-Djava.class.path=%s' % jarpath)
           
            self._connection_string = [jdbc_url, uid, pwd]
            
            driver_not_found = ("HELP: The JDBC driver for IBM dashDB/DB2 could "+
            "not be found. Please download the latest JDBC Driver at the "+
            "following address: 'http://www-01.ibm.com/support/docview.wss?uid=swg21363866' "+
            "and put the file 'db2jcc.jar' or 'db2jcc4.jar' in the CLASSPATH variable "+
            "or in a folder that is in the CLASSPATH variable. Alternatively place "+
            "it in the folder %s"%here)
            
            try:
                self._con = jaydebeapi.connect('com.ibm.db2.jcc.DB2Driver', self._connection_string)
            except:
                print(driver_not_found)
                raise
                #raise IdaDataBaseError(driver_not_found)
             
            if verbose: 
                print("Connection successful!")
                
            #add DB2GSE to the database FUNCTION PATH            
            #query = "SET CURRENT FUNCTION PATH = CURRENT FUNCTION PATH, db2gse"
            #self.ida_query(query)
            #not anymore, reported problems with ODBC
            #better mention DB2GSE explicitly when accessing its functions
                
        # Setting Autocommit and verbose environment variables
        set_autocommit(autocommit)
        set_verbose(verbose)

    ###########################################################################
    #### Data Exploration
    ###########################################################################

    @lazy
    def current_schema(self):
        """
        Get the current user schema name as a string.

        Returns
        -------
        str
            User's schema name.

        Examples
        --------
        >>> idadb.current_schema()
        'DASHXXXXXX'
        """
        query = "SELECT TRIM(CURRENT_SCHEMA) FROM SYSIBM.SYSDUMMY1"
        return self.ida_scalar_query(query)

    def show_tables(self, show_all=False):
        """
        Show tables and views that are available in self. By default, this 
        function shows only tables that belong to a user’s specific schema.

        Parameters
        ----------
        show_all : bool
            If True, all table and view names in the database are returned, 
            not only those that belong to the user's schema.

        Returns
        -------
        DataFrame
             A data frame containing tables and views names in self with some 
             additional information (TABSCHEMA, TABNAME, OWNER, TYPE). 

        Examples
        --------
        >>> ida_db.show_tables()
             TABSCHEMA           TABNAME       OWNER TYPE
        0    DASHXXXXXX            SWISS  DASHXXXXXX    T
        1    DASHXXXXXX             IRIS  DASHXXXXXX    T
        2    DASHXXXXXX     VIEW_TITANIC  DASHXXXXXX    V
        ...
        >>> ida_db.show_tables(show_all = True)
             TABSCHEMA           TABNAME       OWNER TYPE
        0    DASHXXXXXX            SWISS  DASHXXXXXX    T
        1    DASHXXXXXX             IRIS  DASHXXXXXX    T
        2    DASHXXXXXX     VIEW_TITANIC  DASHXXXXXX    V
        2      SYSTOOLS      IDAX_MODELS  DASH101631    A
        ...

        Notes
        -----
        show_tables implements a cache strategy. The cache is stored when the 
        user calls the method with the argument show_all set to True. This 
        improves performance because database table look ups are a very common 
        operation. The cache gets updated each time a table or view is created 
        or refreshed, each time a table or view is deleted, or when a new 
        IdaDataFrame is opened.

        """
        #### DEVELOPERS FIX: UNCOMMENT WHEN DEAD LOCK
        #show_all = False
        ####

        # Try to retrieve the cache
        if show_all:
            cache = self._retrieve_cache("cache_show_tables")
            if cache is not None:
                return cache

        where_part = "WHERE (OWNERTYPE = 'U')"
        if not show_all:
            where_part += ("AND(TABSCHEMA= '%s') " % self.current_schema)

        query = ('SELECT distinct TABSCHEMA, TABNAME,' +
                 ' OWNER, TYPE from SYSCAT.TABLES ' + where_part +
                 ' ORDER BY "TABSCHEMA","TABNAME"')
        data = self.ida_query(query)
        
        # Workaround for some ODBC version which does not get the entire
        # string of the column name in the cursor descriptor. 
        # This is hardcoded, so be careful
        data.columns = ['TABSCHEMA', 'TABNAME', 'OWNER', 'TYPE']
        
        data = self._upper_columns(data)

        # DASHDB FIX: schema "SAMPLES" and "GOSALES" saved with an extra blank,
        # By doing so, we delete the extra blank.
        # Note that this works because all cells are of type string.

        # OLD version, created an unexpected bug in some wrong ODBC version
        # TypeError: unorderable types: bytes() > int()
        # less pythonic, do not use anymore 
        #for col in data:
        #    for index, val in enumerate(data[col]):
        #        data[col][index] = val.strip()

        # Can be done with a one liner :) 
        data = data.apply(lambda x: x.apply(lambda x: x.strip()))

        # Cache the result
        if show_all is True:
            self.cache_show_tables = data

        return data

    def show_models(self):
        """
        Show models that are available in the database.

        Returns
        -------
        DataFrame

        Examples
        --------
        >>> idadb.show_models()
            MODELSCHEMA               MODELNAME       OWNER
        0   DASHXXXXXX  KMEANS_10857_1434974511  DASHXXXXXX
        1   DASHXXXXXX  KMEANS_11726_1434977692  DASHXXXXXX
        2   DASHXXXXXX  KMEANS_11948_1434976568  DASHXXXXXX
        """
        data = self.ida_query("call idax.list_models()")
        # Workaround for some ODBC version which does not get the entire
        # string of the column name in the cursor descriptor. 
        # This is hardcoded, so be careful
        data.columns = ['modelschema', 'modelname', 'owner', 'created', 'state',
       'miningfunction', 'algorithm', 'usercategory']
        data = self._upper_columns(data)
        return data

    def exists_table_or_view(self, objectname):
        """
        Check if a table or view exists in self.

        Parameters
        ----------
        objectname : str
            Name of the table or view to check.

        Returns
        -------
        bool

        Raises
        ------
        TypeError
            The object exists but is not of the expected type.

        Examples
        --------
        >>> idadb.exists_table_or_view("NOT_EXISTING")
        False
        >>> idadb.exists_table_or_view("TABLE_OR_VIEW")
        True
        >>> idadb.exists_table_or_view("NO_TABLE_NOR_VIEW")
        TypeError : "NO_TABLE_NOR_VIEW" exists in schema '?' but of type '?'
        """
        return self._exists(objectname,['T', 'V'])

    def exists_table(self, tablename):
        """
        Check if a table exists in self.

        Parameter
        ---------
        tablename : str
            Name of the table to check.

        Returns
        -------
        bool

        Raises
        ------
        TypeError
            The object exists but is not of the expected type.

        Examples
        --------
        >>> idadb.exists_table("NOT_EXISTING")
        False
        >>> idadb.exists_table("TABLE")
        True
        >>> idadb.exists_table("NO_TABLE")
        TypeError : "tablename" exists in schema "?" but of type '?'
        """
        return self._exists(tablename,['T'])

    def exists_view(self, viewname):
        """
        Check if a view exists in self.

        Parameters
        ----------
        viewname : str
            Name of the view to check.

        Returns
        -------
        bool

        Raises
        ------
        TypeError
            The object exists but is not of the expected type.

        Examples
        --------
        >>> idadb.exists_view("NOT_EXISTING")
        False
        >>> idadb.exists_view("VIEW")
        True
        >>> idadb.exists_view("NO_VIEW")
        TypeError : "viewname" exists in schema "?" but of type '?'
        """
        return self._exists(viewname,['V'])

    def exists_model(self, modelname):
        """
        Check if a model exists in self.

        Parameters
        ----------
        modelname : str
            Name of the model to check.

        Returns
        -------
        bool

        Raises
        ------
        TypeError
            The object exists but is not of the expected type.

        Examples
        --------
        >>> idadb.exists_model("MODEL")
        True
        >>> idadb.exists_model("NOT_EXISTING")
        False
        >>> idadb.exists_model("NO_MODEL")
        TypeError : NO_TABLE exists but is not a model (of type '?')
        """
        modelname = ibmdbpy.utils.check_tablename(modelname)
        if '.' in modelname:
            modelname = modelname.split('.')[-1]

        data = self.show_models()

        if not data.empty:
            if modelname in data['MODELNAME'].values:
                return True

        tablelist = self.show_tables(show_all=True)
        if len(tablelist):
            if modelname in tablelist['TABNAME'].values:
                tabletype = tablelist[tablelist['TABNAME'] == modelname]['TYPE'].values[0]
                raise TypeError("%s exists but is not a model (of type '%s')"
                                %(modelname, tabletype))
            else:
                return False
        else:
            return False

    def is_table_or_view(self, objectname):
        """
        Check if an object is a table or a view in self.

        Parameters
        ----------
        objectname : str
            Name of the object to check.

        Returns
        -------
        bool

        Raises
        ------
        ValueError
            objectname doesn't exist in the database.


        Examples
        --------
        >>> idadb.is_table_or_view("NO_TABLE")
        False
        >>> idadb.is_table_or_view("TABLE")
        True
        >>> idadb.is_table_or_view("NOT_EXISTING")
        ValueError : NO_EXISTING does not exist in database
        """
        return self._is(objectname,['T','V'])

    def is_table(self, tablename):
        """
        Check if an object is a table in self.

        Parameters
        ----------
        tablename : str
            Name of the table to check.

        Returns
        -------
        bool

        Raises
        ------
        ValueError
            The object doesn't exist in the database.


        Examples
        --------
        >>> idadb.is_table("NO_TABLE")
        False
        >>> idadb.is_table("TABLE")
        True
        >>> idadb.is_table("NOT_EXISTING")
        ValueError : NO_EXISTING does not exist in database
        """
        return self._is(tablename,['T'])

    def is_view(self, viewname):
        """
        Check if an object is a view in self.

        Parameters
        ----------
        viewname : str
            Name of the view to check.

        Returns
        -------
        bool

        Raises
        ------
        ValueError
            The object doesn't exist in the database.

        Examples
        --------
        >>> idadb.is_view("NO_VIEW")
        False
        >>> idadb.is_view("VIEW")
        True
        >>> idadb.is_view("NOT_EXISTING")
        ValueError : NO_EXISTING does not exist in database
        """
        return self._is(viewname,['V'])

    def is_model(self, modelname):
        """
        Check if an object is a model in self.

        Parameters
        ----------
        modelname : str
            Name of the model to check.

        Returns
        -------
        bool

        Raises
        ------
        ValueError
            The object doesn't exist in the database.

        Examples
        --------
        >>> idadb.is_model("MODEL")
        True
        >>> idadb.is_model("NO_MODEL")
        False
        >>> idadb.is_model("NOT_EXISTING")
        ValueError : NO_EXISTING doesn't exist in database
        """
        modelname = ibmdbpy.utils.check_tablename(modelname)

        if '.' in modelname:
            modelname_noschema = modelname.split('.')[-1]
        else:
            modelname_noschema = modelname
        data = self.show_models()
        if not data.empty:
            if modelname_noschema in data['MODELNAME'].values:
                return True

        # This part is executed if data is empty or model is not in data
        try:
            self.is_table_or_view(modelname)
        except:
            raise
        else:
            return False

    def ida_query(self, query, silent=False, first_row_only=False, autocommit = False):
        """
        Prepare, execute and format the result of a query in a dataframe or
        in a Tuple. If nothing is expected to be returned for the SQL command,
        nothing is returned. 

        Parameters
        ----------
        query : str
            Query to be executed.
        silent: bool, default: False
             If True, the query is not printed in the python console even if 
             the verbosity mode is activated (VERBOSE environment variable is 
             equal to “True”). 
        first_row_only : bool, default: False
             If True, only the first row of the result is returned as a Tuple.
        autocommit: bool, default: False
            If True, the autocommit function is available.

        Returns
        -------
        DataFrame or Tuple (if first_row_only=False)

        Examples
        --------
        >>> idadb.ida_query("SELECT * FROM IRIS FETCH FIRST 5 ROWS ONLY")
           sepal_length  sepal_width  petal_length  petal_width species
        0           5.1          3.5           1.4          0.2  setosa
        1           4.9          3.0           1.4          0.2  setosa
        2           4.7          3.2           1.3          0.2  setosa
        3           4.6          3.1           1.5          0.2  setosa
        4           5.0          3.6           1.4          0.2  setosa

        >>> idadb.ida_query("SELECT COUNT(*) FROM IRIS")
        (150, 150, 150, 150)

        Notes
        -----
        If first_row_only argument is True, then even if the actual result of 
        the query is composed of several rows, only the first row will be 
        returned.
        """
        self._check_connection()
        return sql.ida_query(self, query, silent, first_row_only, autocommit)

    def ida_scalar_query(self, query, silent=False, autocommit = False):
        """
        Prepare and execute a query and return only the first element as a 
        string. If nothing is returned from the SQL query, an error occur.

        Parameters
        ----------
        query : str
            Query to be executed.
        silent: bool, default: False
            If True, the query will not be printed in python console even if
            verbosity mode is activated.
        autocommit: bool, default: False
            If True, the autocommit function is available.

        Returns
        -------
        str or Number

        Examples
        --------
        >>> idadb.ida_scalar_query("SELECT TRIM(CURRENT_SCHEMA) from SYSIBM.SYSDUMMY1")
        'DASHXXXXX'

        Notes
        -----
        Even if the actual result of the query is composed of several columns
        and several rows, only the first element (top-left) will be returned.
        """
        self._check_connection()
        return sql.ida_scalar_query(self, query, silent, autocommit)

    ###############################################################################
    #### Upload DataFrames
    ###############################################################################

    @timed
    def as_idadataframe(self, dataframe, tablename=None, clear_existing=False, primary_key=None, indexer=None):
        """
        Upload a dataframe and return its corresponding IdaDataFrame. The target
        table (tablename) will be created or replaced if the option clear_existing
        is set to True.
        
        To add data to an existing tables, see IdaDataBase.append

        Parameters
        ----------
        dataframe : DataFrame
            Data to be uploaded, contained in a Pandas DataFrame.
        tablename : str, optional
            Name to be given to the table created in the database. If not 
            given, a valid tablename is generated (for example, DATA_FRAME_X 
            where X a random number).
        clear_existing : bool
            If set to True, a table will be replaced when a table with the same 
            name already exists  in the database.
        primary_key : str
            Name of a column to be used as primary key.

        Returns
        -------
        IdaDataFrame

        Raises
        ------
        TypeError
            * Argument dataframe is not of type pandas.DataFrame.
            * The primary key argument is not a string.

        NameError
            * The name already exists in the database and clear_existing is False.
            * The primary key argument doesn't correspond to a column.

        PrimaryKeyError
            The primary key contains non unique values.

        Examples
        --------
        >>> from ibmdbpy.sampledata.iris import iris
        >>> idadb.as_idadataframe(iris, "IRIS")
        <ibmdbpy.frame.IdaDataFrame at 0xb34a898>
        >>> idadb.as_idadataframe(iris, "IRIS")
        NameError: IRIS already exists, choose a different name or use clear_existing option.
        >>> idadb.as_idadataframe(iris, "IRIS2")
        <ibmdbpy.frame.IdaDataFrame at 0xb375940>
        >>> idadb.as_idadataframe(iris, "IRIS", clear_existing = True)
        <ibmdbpy.frame.IdaDataFrame at 0xb371cf8>
        
        Notes
        -----
        This function is not intended to be used to add data to an existing table,
        rather to create a new table from a dataframe. To add data to an existing
        table, please consider using IdaDataBase.append 
        """
        if not isinstance(dataframe, pd.DataFrame):
            raise TypeError("Argument dataframe is not of type Pandas.DataFrame")

        if tablename is None:
                tablename = self._get_valid_tablename(prefix="DATA_FRAME_")

        tablename = ibmdbpy.utils.check_tablename(tablename)

        if primary_key is not None:
            if not isinstance(primary_key, six.string_types):
                raise TypeError("The primary key argument should be a string")
            if primary_key not in dataframe.columns:
                raise ValueError("The primary key should be the name of a column" +
                                 " in the given dataframe")
            if len(dataframe[primary_key]) != len(set(dataframe[primary_key])):
                raise PrimaryKeyError(primary_key + " cannot be a primary key for" +
                                      "table " + tablename + " because it contains" +
                                      " non unique values")

        if self.exists_table_or_view(tablename):
            if clear_existing:
                try:
                    self.drop_table(tablename)
                except:
                    self.drop_view(tablename)
            else:
                raise NameError(("%s already exists, choose a different name "+
                                "or use clear_existing option.")%tablename)

        self._create_table(dataframe, tablename, primary_key=primary_key)
        idadf = ibmdbpy.frame.IdaDataFrame(self, tablename, indexer)
        self.append(idadf, dataframe)

        ############## Experimental ##################
        # dataframe.to_sql(tablename, self._con, index=False)
        # idadf = ibmdbpy.frame.IdaDataFrame(self, tablename, flavor='mysql',
        #                                    schema = self.current_schema)

        self._autocommit()

        if primary_key:
            idadf.indexer = primary_key
        return idadf
        
    ###########################################################################
    #### Delete DataBase objects
    ###########################################################################

    def drop_table(self, tablename):
        """
        Drop a table in the database.

        Parameters
        ----------
        tablename : str
            Name of the table to drop.

        Raises
        ------
        ValueError
            If the object does not exist.
        TypeError
            If the object is not a table.

        Examples
        --------
        >>> idadb.drop_table("TABLE")
        True
        >>> idadb.drop_table("NO_TABLE")
        TypeError : NO_TABLE  exists in schema '?' but of type '?'
        >>> idadb.drop_table("NOT_EXISTING")
        ValueError : NO_EXISTING doesn't exist in database

        Notes
        -----
            This operation cannot be undone if autocommit mode is activated.
        """
        return self._drop(tablename, "T")

    def drop_view(self, viewname):
        """
        Drop a view in the database.

        Parameters
        ----------
        viewname : str
            Name of the view to drop.

        Raises
        ------
        ValueError
            If the object does not exist.
        TypeError
            If the object is not a view.

        Examples
        --------
        >>> idadb.drop_view("VIEW")
        True
        >>> idadb.drop_view("NO_VIEW")
        TypeError : NO_VIEW exists in schema '?' but of type '?'
        >>> idadb.drop_view("NOT_EXISTING")
        ValueError : NO_EXISTING doesn't exist in database

        Notes
        -----
            This operation cannot be undone if autocommit mode is activated.
        """
        return self._drop(viewname, "V")

    def drop_model(self, modelname):
        """
        Drop a model in the database.

        Parameters
        ----------
        modelname : str
            Name of the model to drop.

        Raises
        ------
        ValueError
            If the object does not exist.
        TypeError
            if the object exists but is not a model.

        Examples
        --------
        >>> idadb.drop_model("MODEL")
        True
        >>> idadb.drop_model("NO_MODEL")
        TypeError : NO_MODEL exists in schema '?' but of type '?'
        >>> idadb.drop_model("NOT_EXISTING")
        ValueError : NOT_EXISTING does not exists in database

        Notes
        -----
            This operation cannot be undone if autocommit mode is activated.

        """
        model = "\"" + self.current_schema + "\".\"" + modelname + "\""

        try:
            self._prepare_and_execute("CALL IDAX.DROP_MODEL('model=" + model + "')")
        except:
            try:
                flag = self.exists_table(modelname)
            except TypeError:
                # Exists but is not a model (nor a table)
                raise
            else:
                if flag:
                    # It is a table so make it raise by calling exists_view
                    self.exists_view(modelname)
            raise ValueError(modelname + " does not exists in database")
        else:
            tables = self.show_tables()
            if not tables.empty:
                for table in tables['TABNAME']:
                    if modelname in table:
                        self.drop_table(table)
            return True

    @timed
    def rename(self, idadf, newname):
        """
        Rename a table referenced by an IdaDataFrame in dashDB/DB2.

        Parameters
        ----------
        idadf : IdaDataFrame
            IdaDataFrame object referencing the table to rename.
        newname : str
            Name to be given to self. Should contain only alphanumerical 
            characters and underscores. All lower case characters will be 
            converted to upper case characters. The new name should not already 
            exist in the database.

        Raises
        ------
        ValueError
            The new tablename is not valid.
        TypeError
            Rename function is supported only for table type.
        NameError
            The name of the object to be created is identical to an existing name.

        Notes
        -----
            Upper case characters and numbers, optionally separated by 
            underscores “_”, are valid characters.
        """
        # Actually we could support it for views too
        # Question : Is it better to accept an idadf as argument or rather the name of the table?
        oldname = idadf._name
        newname = ibmdbpy.utils.check_tablename(newname)

        if self.is_table(idadf._name):
            query = "RENAME TABLE %s TO %s"%(idadf._name, newname)
            try:
                self._prepare_and_execute(query)
            except Exception as e:
                if self._con_type == "odbc":
                    raise NameError(e.value[-1])
                else:
                    sql_code = int(str(e.args[0]).split("SQLCODE=")[-1].split(",")[0])
                    if sql_code == -601:
                        raise NameError("The new name is identical to the old one")
                    else:
                        raise e

            idadf._name = newname
            idadf.tablename = newname
            idadf.internal_state.name = newname
            self._reset_attributes("cache_show_tables")

            # Update name of all IdaDataFrame that were opened on this table
            for idadf in self._idadfs:
                if idadf._name == oldname:
                    idadf._name = newname
                    idadf.internal_state.name = newname # to refactor
        else:
            raise TypeError("Rename function is supported only for table type")

    def add_column(self, idadf, column = None, colname = None, ncat=10):
        """
        Add physically a column to the dataset.
        If column argument is set to None, a random column is generated which 
        can take values for 1 to ncat.
        Used for benchmark purpose. 
        Note: This method is deprecated. Can be probably be removed without impact 
        """
        if column is not None:
            if column not in idadf.columns:
                raise ValueError("Unknown column %s"%column)
            dtype = idadf.dtypes['TYPENAME'][column]
            if dtype == "VARCHAR":
                dtype = dtype + "(255)"
        else:
            dtype = "INTEGER"
        
        if colname is None:
            i = 0
            colname = "COL%s"%i
            while colname in idadf.columns:
                i += 1
                colname = "COL%s"%i
        else:
            if colname in idadf.columns:
                raise ValueError("Column %s already exists"%colname)
        
        self.commit() 
        query = "ALTER TABLE %s ADD \"%s\" %s"%(idadf._name, colname, dtype) 
        print(query)         
        try:
            self._prepare_and_execute(query)
        except:
            print("Failed ! Trying again in 5 seconds")
            #query2 = "REORG TABLE %s"%idadf._name
            #print(query2)
            import time
            time.sleep(5)
            self._prepare_and_execute(query)
        self.commit()
        
        # force sleep 2 sec
        
        
        if column is None:
            # Random column
            query = "UPDATE %s SET \"%s\" = RAND()* %s + 1"%(idadf._name, colname, ncat)
        else:
            query = "UPDATE %s SET \"%s\" = \"%s\""%(idadf._name, colname, column)
        
        print(query)
        
        try:
            self._prepare_and_execute(query)
        except:
            print("Failed ! Trying again in 5 seconds")
            import time
            time.sleep(5)
            self._prepare_and_execute(query)
                
            
        self.commit()
        idadf._reset_attributes(["columns", "dtypes", "shape"])
        
            
    
    @timed
    def add_column_id(self, idadf, column_id="ID", destructive=False):
        # TODO: Base the creation of the idea on the sorting of several columns
        # (or all columns in case there are duplicated rows) so that the ID
        # can be created in a determinstic and reproducible way
        """
        Add an ID column to an IdaDataFrame.

        Arguments
        ---------
        idadf : IdaDataFrame
            IdaDataFrame object ot which a column id will be added
        column_id : str
            Name of the column id to add
        destructive : bool
            If set to True, the column will be added phisically in the database.
            This can take time. If set to False, the column will be added virtually
            in a view and a new IdaDataFrame is returned

        Raises
        ------
        TypeError
            idadf is not an IdaDataFrame.
        ValueError
            The given column name already exists in the DataBase.

        Notes
        -----
            The non-destructive creation of column IDs is not reliable, because 
            row IDs are recalculated on the fly in a non-deterministic way. The 
            only reliable way is to create it destructively, but the row IDs 
            will be created at random. This could be highly improved in the 
            future. An idea is to create ID columns in a non-destructive way 
            and base them on the sorting of a set of columns, defined by the 
            user, or all columns if no column combination results in unique 
            identifiers.
            
            This method is DEPRECATED. 
        """
        if isinstance(idadf, ibmdbpy.IdaSeries):
            raise TypeError("Adding column ID is not supported for IdaSeries")
        if not isinstance(idadf, ibmdbpy.IdaDataFrame):
            raise TypeError("idadf is not an IdaDataFrame, type: %s"%type(idadf))
        if column_id in idadf.columns:
            raise ValueError("A column named '"+column_id+"' already exists." +
                             " Please define a new column name using"+
                             " column_id argument")

        if destructive is True:

            viewname = self._get_valid_tablename(prefix="VIEW_")
            self._prepare_and_execute("CREATE VIEW " + viewname + " AS SELECT ((ROW_NUMBER() OVER())-1)" +
                                      " AS \"" + column_id + "\", \""+
                                      "\",\"".join(idadf._get_all_columns_in_table()) +
                                      "\" FROM " + idadf._name)

            # Initiate the modified table under a random name
            tablename = self._get_valid_tablename(prefix="DATA_FRAME_")
            self._prepare_and_execute("CREATE TABLE %s LIKE %s"%(tablename,viewname))
            self._prepare_and_execute("INSERT INTO %s (SELECT * FROM %s)"%(tablename,viewname))

            # Drop the view and old table
            self.drop_view(viewname)
            self.drop_table(idadf._name)

            # Give it the original name back
            self._reset_attributes("cache_show_tables")
            new_idadf = ibmdbpy.IdaDataFrame(self, tablename)
            self.rename(new_idadf, idadf.tablename)

            # Updating internal state
            # prepend the columndict OrderedDict
            items = idadf.internal_state.columndict.items()
            idadf.internal_state.columndict = OrderedDict()
            idadf.internal_state.columndict[column_id] = "\"" + column_id + "\""
            for item in items:
                idadf.internal_state.columndict[item[0]] = item[1]

            idadf.internal_state.update()
            idadf._reset_attributes(["columns"])
            idadf.indexer = column_id
        else:
            # prepend the columndict OrderedDict
            items = idadf.internal_state.columndict.items()
            idadf.internal_state.columndict = OrderedDict()
            idadf.internal_state.columndict[column_id] = "((ROW_NUMBER() OVER())-1)"
            for item in items:
                idadf.internal_state.columndict[item[0]] = item[1]
            idadf.internal_state.update()
            idadf._reset_attributes(["columns"])
            idadf.indexer = column_id

        # Reset attributes
        idadf._reset_attributes(['shape', 'columns', 'axes', 'dtypes'])

    def delete_column(self, idadf, column_name, destructive=False):
        """
        Delete a column in an idaDataFrame.

        Parameters
        ----------
        idadf : IdaDataFrame
            The IdaDataframe in which a column should be deleted.
        column_name : str
            Name of the column to delete.
        destructive : bool
            If set to True, the column is deleted in the database. Otherwise, 
            it is deleted virtually, creating a view for the IdaDataFrame.

        Raises
        ------
        TypeError
            column_name should be a string.
        ValueError
            column_name refers to a column that doesn't exist in self.
        """
        if not isinstance(column_name, six.string_types):
            raise TypeError("column_name is not of string type")
        if column_name not in idadf.columns:
            raise ValueError("%s refers to a columns that doesn't exists in self"%(column_name))

        if destructive is True:
            if column_name not in idadf._get_all_columns_in_table():
                # Detect it is a virtual ID, the deletion cannot be destructive
                return self.delete_column(idadf, column_name, destructive=False)

            viewname = self._get_valid_tablename(prefix="VIEW_")
            columnlist = list(idadf._get_all_columns_in_table())
            columnlist.remove(column_name)

            self._prepare_and_execute("CREATE VIEW " + viewname + " AS SELECT \""+
                                      "\",\"".join(columnlist) +
                                      "\" FROM " + idadf._name)

            tablename = self._get_valid_tablename(prefix="DATA_FRAME_")

            self._prepare_and_execute("CREATE TABLE " + tablename + " LIKE " + viewname)
            self._prepare_and_execute("INSERT INTO " + tablename + " (SELECT * FROM " + viewname + ")")

            # Drop the view and old table
            self.drop_view(viewname)
            self.drop_table(idadf._name)

            # Give it the original name back
            self._reset_attributes("cache_show_tables") # normally, no needed
            new_idadf = ibmdbpy.IdaDataFrame(self, tablename, idadf.indexer)
            self.rename(new_idadf, idadf.tablename)

            # updating internal state
            del idadf.internal_state.columndict[column_name]
            idadf.internal_state.update()
            self._reset_attributes("cache_show_tables")
            idadf._reset_attributes(['shape', 'columns', 'dtypes'])

        else:
            del idadf.internal_state.columndict[column_name]
            idadf.internal_state.update()
            idadf._reset_attributes(["columns", "shape", "dtypes"])

        if column_name == idadf.indexer:
            idadf._reset_attributes(["_indexer"])


    def append(self, idadf, df, maxnrow=None):
        """
        Append rows of a DataFrame to an IdaDataFrame. The DataFrame must have 
        the same structure (same column names and datatypes). Optionally, the 
        DataFrame to be added can be splitted into several chunks. This 
        improves performance and prevents SQL overflows. By default, chunks are 
        limited to 8000 cells.

        Parameters
        ----------
        idadf : IdaDataFrame
            IdaDataFrame that receives data from dataframe df.
        df : DataFrame
            Dataframe whose rows are added to IdaDataFrame idadf.
        maxnrow : int, optional
            number corresponding to the maximum number of rows for each chunks.

        Raises
        ------
        TypeError
            * maxnrow should be an interger.
            * Argument idadf should be an IdaDataFrame.
            * Argument df should be a pandas DataFrame.
        ValueErrpr
            * maxnrow should be greater than 1 or nleft blank.
            * Other should be a Pandas DataFrame.
            * Other dataframe has not the same number of columns as self.
            * Some columns in other have different names that are different from the names of the columns in self.
        """
        # SANITY CHECK : maxnrow
        if maxnrow is None:
            # Note : 8000 is an empirical maximum number of cells
            maxnrow = int(8000 / len(df.columns))
        else:
            if not isinstance(maxnrow, six.integer_types):
                raise TypeError("maxnrow is not an integer")
            if maxnrow < 1:
                raise ValueError("maxnrow should be stricly positive or omitted")
            if maxnrow > 15000:
                warnings.warn("Performance may decrease if maxnrow is bigger than 15000", UserWarning)

        # SANITY CHECK : idadf & other
        if not isinstance(idadf, ibmdbpy.frame.IdaDataFrame):
            raise TypeError("Argument idadf is not an IdaDataFrame")
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Argument df is not of type pPandas.DataFrame")
        if len(df.columns) != len(idadf.columns):
            raise ValueError("(Ida)DataFrames don't have the same number of columns")
        if any([column not in idadf.columns for column in [str(x).strip() for x in df.columns]]):
            raise ValueError("Some column names do not match current object columns: \n" +
                             "Expected : \t" + str(idadf.columns) + "\n" +
                             "Found : \t" + str([x.strip() for x in df.columns]) + "\n")
        if any([str(column_idadf) != str(column_other).strip() for column_idadf, column_other in
                zip(idadf.columns, df.columns)]):
            raise ValueError("Order or columns in other and" + idadf._name + "does not match.")

        if df.shape[0] > 1.5 * maxnrow:
            split_into = math.ceil(df.shape[0] / maxnrow)
            split = np.array_split(df, split_into)
            print("DataFrame will be splitted into " + str(split_into) +
                  " chunks. (" + str(maxnrow) + " rows per chunk)")
            for i, chunk in enumerate(split, 0):
                percentage = int(i / split_into * 100)
                print("Uploaded: " + str(percentage) + "%... ", end="\r")
                try:
                    self._insert_into_database(chunk, idadf.tablename, silent=True)
                except:
                    raise
            print("Uploaded: %s/%s... "%(split_into,split_into), end="")
            print("[DONE]")
        else:
            print("Uploading %s rows (maxnrow was set to %s)"%(df.shape[0], maxnrow))
            try:
                self._insert_into_database(df, idadf.tablename, silent=True)
            except:
                raise

        idadf._reset_attributes(['shape', 'axes', 'dtypes', 'index'])

    def merge(self, idadf, other, key):
        # TODO:
        pass

    ###############################################################################
    #### Connection management
    ###############################################################################

    def commit(self):
        """
        Commit operations in the database.

        Notes
        -----

        All changes that are made in the database after the last commit, 
        including those in the child IdaDataFrames, are commited.

        If the environment variable ‘VERBOSE’ is set to True, the commit 
        operations are notified in the console.
        """
        self._check_connection()  # Important
        self._con.commit()
        if os.getenv('VERBOSE') == 'True':
            print("<< COMMIT >>")
        self._reset_attributes("cache_show_tables")

    def rollback(self):
        """
        Rollback operations in the database.

        Notes
        -----

        All changes that are made in the database after the last commit, 
        including those in the child IdaDataFrames, are discarded.
        """
        self._check_connection()  # Important
        self._con.rollback()
        if os.getenv('VERBOSE') == 'True':
            print("<< ROLLBACK >>")
        self._reset_attributes("cache_show_tables")

    def close(self):
        """
        Close the IdaDataBase connection.

        Notes
        -----

        If the environment variable ‘AUTOCOMMIT’ is set to True, then all 
        changes after the last commit are committed, otherwise they are 
        discarded.

        """
        if os.getenv('AUTOCOMMIT') == 'True':
            self.commit()
        else:
            self.rollback()
        self._reset_attributes("cache_show_tables")
        self._con.close()
        print("Connection closed.")

    def reconnect(self):
        """
        Try to reopen the connection.
        """
        try:
            self._check_connection()
        except IdaDataBaseError:
            if self._con_type == 'odbc':
                import pypyodbc
                try:
                    self._con = pypyodbc.connect(self._connection_string)
                except:
                    raise
                else:
                    print("The connection was successfully restored")
            elif self._con_type == 'jdbc':
                try:
                    import jaydebeapi
                    self._con = jaydebeapi.connect('com.ibm.db2.jcc.DB2Driver', self._connection_string)
                except:
                    raise
                else:
                    print("The connection was successfully restored")
        else:
            print("The connection for current IdaDataBase is valid")

        ###############################################################################
        #### Private methods
        ###############################################################################

    def __enter__(self):
        """
        Allow the object to be used with a "with" statement
        """
        return self

    def __exit__(self):
        """
        Allow the object to be used with a "with" statement. Make sure the
        connection is closed when the object get out of scope
        """
        self.close()

    def _exists(self, objectname, typelist):
        """
        Check if an object of a certain type exists in dashDB/DB2.

        Notes
        -----
        For more information, see exists_table_or_view, exists_table, 
        exists_view functions.
        """
        objectname = ibmdbpy.utils.check_tablename(objectname)

        tablelist = self.show_tables(show_all=True)
        schema, name = self._get_name_and_schema(objectname)
        tablelist = tablelist[tablelist['TABSCHEMA'] == schema]

        if len(tablelist):
            if name in tablelist['TABNAME'].values:
                tabletype = tablelist[tablelist['TABNAME'] == name]['TYPE'].values[0]
                if tabletype in typelist:
                    return True
                else:
                    raise TypeError("%s exists in schema %s but of type \"%s\""
                                    %(objectname,schema,tabletype))
            else:
                return False
        else:
            return False

    def _is(self, objectname, typelist):
        """
        Check if an existing object is of a certain type or in a list of types.

        Notes
        -----
        For more information, see is_table_or_view, is_table, is_view functions.
        """
        objectname = ibmdbpy.utils.check_tablename(objectname)

        tablelist = self.show_tables(show_all=True)
        schema, name = self._get_name_and_schema(objectname)
        tablelist = tablelist[tablelist['TABSCHEMA'] == schema]

        if len(tablelist):
            if name in tablelist['TABNAME'].values:
                tabletype = tablelist[tablelist['TABNAME'] == name]['TYPE'].values[0]
                if tabletype in typelist:
                    return True
                else:
                    return False

        raise ValueError("%s does not exists in database"%(objectname))

    def _drop(self, objectname, object_type = "T"):
        """
        Drop an object in the table depending on its type.
        Admissible type values are "T" (table) and "V" (view)

        Notes
        -----
        For more information, seedrop_table and drop_view functions.

        """
        objectname = ibmdbpy.utils.check_tablename(objectname)

        if object_type == "T":
            to_drop = "TABLE"
        elif object_type == "V":
            to_drop = "VIEW"
        else:
            raise ValueError("Unknown type to drop")

        try:
            self._prepare_and_execute("DROP %s %s"%(to_drop,objectname))
        except Exception as e:
            if self._con_type == "odbc":
                if e.value[0] == "42S02":
                    raise ValueError(e.value[1])  # does not exists
                if e.value[0] == "42809":
                    raise TypeError(e.value[1])  # object is not of expected type
            else:
                sql_code = int(str(e.args[0]).split("SQLCODE=")[-1].split(",")[0])
                if sql_code == -204:
                    raise ValueError("Object does not exists")
                elif sql_code == -159:
                    raise TypeError("Object is not of expected type")
                else:
                    raise e # let the expection raise anyway
        else:
            self._reset_attributes("cache_show_tables")
            return True

    def _upper_columns(self, dataframe):
        # Could be moved to utils (then move in the test too)
        """
        Put every column name of a Pandas DataFrame in upper case.

        Arguments
        ---------
        dataframe: DataFrame
        Returns
        -------
        DataFrame
        """
        data = deepcopy(dataframe)
        if len(data):
            data.columns = [x.upper() for x in data.columns]
        return data

    def _get_name_and_schema(self, objectname):
        """
        Helper function that returns the name and the schema from an object 
        name. Implicitly, if no schema name was given, it is assumed that user 
        refers to the current schema.


        Arguments
        ---------
        objectname : str
            Name of the object to process. Can be either under the form
            "SCHEMA.TABLE" or just "TABLE"

        Returns
        -------
        tuple
            A tuple composed of 2 strings containing the schema and the name.

        Examples
        --------
        >>> _get_name_and_schema(SCHEMA.TABLE)
        (SCHEMA, TABLE)
        >>> _get_name_and_schema(TABLE)
        (<current schema>, TABLE)
        """
        if '.' in objectname:
            name = objectname.split('.')[-1]
            schema = objectname.split('.')[0]
        else:
            name = objectname
            schema = self.current_schema
        return (schema, name)

    def _get_valid_tablename(self, prefix="DATA_FRAME_"):
        """
        Generate a valid database table name.

        Parameters
        ----------
        prefix : str, default: "DATA_FRAME_"
            Prefix used to create the table name. The name is constructed using 
            this pattern : <prefix>_X where <prefix> corresponds to the string 
            parameter “prefix” capitalized and X corresponds to a pseudo 
            randomly generated number (0-100000).

        Returns
        -------
        str

        Examples
        --------
        >>> idadb._get_valid_tablename()
        'DATA_FRAME_49537_1434978215'
        >>> idadb._get_valid_tablename("MYDATA_")
        'MYDATA_65312_1434978215'
        >>> idadb._get_valid_tablename("mydata_")
        'MYDATA_78425_1434978215'
        >>> idadb._get_valid_tablename("mydata$")
        ValueError: Table name is not valid, only alphanumeric characters and underscores are allowed.
        """
        prefix = ibmdbpy.utils.check_tablename(prefix)
        # We may assume that the generated name is unlikely to exist
        name = "%s%s_%s" % (prefix, random.randint(0, 100000), int(time()))
        return name

    def _get_valid_viewname(self, prefix="VIEW_"):
        """
        Convenience function : Alternative name for get_valid_tablename.

        The parameter prefix has its optional value changed to "VIEW_".

        Examples
        --------
        >>> idadb._get_valid_viewname()
        'VIEW_49537_1434978215'
        >>> idadb._get_valid_viewname("MYVIEW_")
        'MYVIEW_65312_1434978215'
        >>> idadb._get_valid_viewname("myview_")
        'MYVIEW_78425_1434978215'
        >>> idadb._get_valid_modelname("myview$")
        ValueError: View name is not valid, only alphanumeric characters and underscores are allowed.
        """
        return self._get_valid_tablename(prefix)

    def _get_valid_modelname(self, prefix="MODEL_"):
        """
        Convenience function : Alternative name for get_valid_tablename.

        Parameter prefix has its optional value changed to "MODEL_".

        Examples
        --------
        >>> idadb._get_valid_modelname()
        'MODEL_49537_1434978215'
        >>> idadb._get_valid_modelname("TEST_")
        'TEST_65312_1434996318'
        >>> idadb._get_valid_modelname("test_")
        'TEST_78425_1435632423'
        >>> idadb._get_valid_tablename("mymodel$")
        ValueError: Table name is not valid, only alphanumeric characters and underscores are allowed.
        """
        return self._get_valid_tablename(prefix)

    def _create_table(self, dataframe, tablename, primary_key=None):
        """
        Create a new table in the database by declaring its name and columns 
        based on an existing DataFrame. It is possible declare a column as 
        primary key.

        Parameters
        ----------
        dataframe : DataFrame
            Pandas DataFrame be used to initiate the table.
        tablename : str
            Name to be given to the table at its creation.
        primary_key: str
            Name of a column to declare as primary key.

        Notes
        -----
        The columns and their data type is deducted from the Pandas DataFrame 
        given as parameter.

        Examples
        --------
        >>> from ibmdbpy.sampledata.iris import iris
        >>> idadb._create_table(iris, "IRIS")
        'IRIS'
        >>> idadb._create_table(iris)
        'DATA_FRAME_4956'
        """
        # TODO : Handle more types
        # integer_attributes =
        # ['int_', 'intc','BIGINT','REAL','DOUBLE','FLOAT','DECIMAL','NUMERIC']
        # double_attributes =
        # ['SMALLINT', 'INTEGER','BIGINT','REAL','DOUBLE','FLOAT','DECIMAL','NUMERIC']
        # self._check_connection()

        if not isinstance(dataframe, pd.DataFrame):
            raise TypeError("_create_table is valid only for DataFrame objects")

        if primary_key is not None:
            if not isinstance(primary_key, six.string_types):
                raise TypeError("primary_key argument should be a string")

        # Check the tablename
        tablename = ibmdbpy.utils.check_tablename(tablename)

        column_string = ''
        for column in dataframe.columns:
            if dataframe.dtypes[column] in [object,bool]:
                # Handle boolean type
                if set(dataframe[column].unique()).issubset([True, False, 0, 1, np.nan]):
                    column_string += "\"%s\" SMALLINT," % str(column).strip()
                else:
                    if column == primary_key:
                        column_string += "\"%s\" VARCHAR(255) NOT NULL, PRIMARY KEY (\"%s\")," % (str(column).strip(), str(column).strip())
                    else:
                        column_string += "\"%s\" VARCHAR(255)," % str(column).strip()
            elif dataframe.dtypes[column] == np.dtype('datetime64[ns]'):
                # This is a first patch for handling dates
                # TODO: Dates as timestamp in the database
                column_string += "\"%s\" VARCHAR(255)," % str(column).strip()
            else:
                if dataframe.dtypes[column] in [np.int64, np.int, np.int8, np.int32]:
                    if abs(dataframe[column].max()) < 2147483647/2: # might get bigger 
                        datatype = "INT"
                    else:
                        datatype = "BIGINT"
                else:
                    datatype = "DOUBLE"
                if column == primary_key:
                    column_string += "\"%s\" %s NOT NULL, PRIMARY KEY (\"%s\")," % (str(column).strip(), datatype, str(column).strip())
                else:
                    column_string += "\"%s\" %s," %(str(column.strip()), datatype)
        if column_string[-1] == ',':
            column_string = column_string[:-1]

        create_table = "CREATE TABLE \"%s\" (%s)" % (tablename, column_string)
        self._prepare_and_execute(create_table, autocommit=False)

        # Save new table in cache
        if hasattr(self, "cache_show_tables"):
            record = (self.current_schema, tablename, self.current_schema, 'T')
            self.cache_show_tables.loc[len(self.cache_show_tables)] = np.array(record)

        return tablename

    def _create_view(self, idadf, viewname = None):
        """
        Create a new view in the database from an existing table.

        Parameters
        ----------

        idadf : IdaDataFrame
            IdaDataFrame to be duplicated as view.

        viewname : str, optional
            Name to be given to the view at its creation. If not given, a 
            random name will be generated automatically 

        Returns
        -------
        str
            View name.

        Examples
        --------
        >>> idadf = IdaDataFrame(idadb, "IRIS")
        >>> idadb._create_view(idadf)
        'IDAR_VIEW_4956'
        """
        if not isinstance(idadf, ibmdbpy.frame.IdaDataFrame):
            raise TypeError("_create_view is valid only for IdaDataFrame objects")

        # Check the viewname
        if viewname is not None:
            viewname = ibmdbpy.utils.check_viewname(viewname)
        else:
            viewname = self._get_valid_viewname()

        self._prepare_and_execute("CREATE VIEW \"%s\" AS SELECT * FROM %s" % (viewname, idadf._name))
    
        # Save new view in cache
        if hasattr(self, "cache_show_tables"):
            record = (self.current_schema, viewname, self.current_schema, 'V')
            self.cache_show_tables.loc[len(self.cache_show_tables)] = np.array(record)
    
        return viewname
        
    def _create_view_from_expression(self, expression, viewname = None):
        """
        Create a new view in the database from an expression.

        Parameters
        ----------

        expression : str
            Expression that defines the view to be created

        viewname : str, optional
            Name to be given to the view at its creation. If not given, a 
            random name will be generated automatically 

        Returns
        -------
        str
            View name.

        Examples
        --------
        TODO
        """
        if not isinstance(expression, six.string_types):
            raise TypeError("expression argument expected to be of string type")

        # Check the viewname
        if viewname is not None:
            viewname = ibmdbpy.utils.check_viewname(viewname)
        else:
            viewname = self._get_valid_viewname()

        self._prepare_and_execute("CREATE VIEW \"%s\" AS %s" % (viewname, expression))
    
        # Save new view in cache
        if hasattr(self, "cache_show_tables"):
            record = (self.current_schema, viewname, self.current_schema, 'V')
            self.cache_show_tables.loc[len(self.cache_show_tables)] = np.array(record)
    
        return viewname

    def _insert_into_database(self, dataframe, tablename, silent=True):
        """
        Populate an existing table with data from a dataframe.

        Parameters
        ----------
        dataframe: DataFrame
            Data to be inserted into an existing table, contained in a Pandas 
            DataFrame. It is assumed that the structure matches.
        tablename: str
            Name of the table in which the data is inserted.
        silent : bool, default: True
            If True, the INSERT statement is not printed. Avoids flooding the 
            console.

        """
        # TODO : Handle more datatypes
        tablename = ibmdbpy.utils.check_tablename(tablename)
        column_string = '\"%s\"' % '\", \"'.join([str(x).strip() for x in dataframe.columns])
        row_string = ''

        # Save in a list columns that are booleans
        boolean_flaglist = []
        for column in dataframe.columns:
            if set(dataframe[column].unique()).issubset([True, False, 0, 1, np.nan]):
                boolean_flaglist.append(1)
            else:
                boolean_flaglist.append(0)

        for rows in dataframe.values:
            value_string = ''
            for colindex, value in enumerate(rows):
                if pd.isnull(value): # handles np.nan and None
                    value_string += "NULL,"  # Handle missing values
                elif isinstance(value, six.string_types):
                    ## Handle apostrophe in values
                    value = value.replace("\\", "'")
                    value_string += '\'%s\',' % value.replace("'", "''")
                # REMARK: it is the best way to handle booleans ?
                elif isinstance(value, bool):
                    if boolean_flaglist[colindex] == True:
                        if value in [1, True]:
                            value_string += '1,'
                        elif value in [0, False]:
                            value_string += '0,'
                    else:
                        value_string += '\'%s\',' % value
                # TODO: Handle datetime better than strings
                elif isinstance(value, datetime.datetime):
                    value_string += '\'%s\',' % value
                else:
                    value_string += '%s,' % value
            if value_string[-1] == ',':
                value_string = value_string[:-1]
            row_string += "(%s)," % value_string
        if row_string[-2:] == '),':
            row_string = row_string[:-2]
        if row_string[0] == '(':
            row_string = row_string[1:]

        query = ("INSERT INTO \"%s\" (%s) VALUES (%s)" % (tablename, column_string, row_string))

        #print(query)
        # TODO: Good idea : create a savepoint before creating the table
        # Rollback in to savepoint in case of failure
        self._prepare_and_execute(query, autocommit=False, silent=silent)

        for idadf in self._idadfs:
            if idadf._name == tablename:
                idadf._reset_attributes(["shape", "index"])

    def _prepare_and_execute(self, query, autocommit=True, silent=False):
        """
        Prepare and execute a query by using the cursor of an idaobject.

        Parameters
        ----------
        idaobject: IdaDataBase or IdaDataFrame
        query: str
            Query to be executed.
        autocommit: bool, default: True
            If True, the autocommit function is available.
        silent: bool, default: False
            If True, the SQL statement is not printed.
        """
        self._check_connection()
        return sql._prepare_and_execute(self, query, autocommit, silent)

    def _check_procedure(self, proc_name, alg_name=None):
        """
        Check if a procedure is available in the database.

        Parameters
        ----------
        proc_name : str
            Name of the procedure to be checked as defined in the underlying
            database management system.
        alg_name : str
            Name of the algorithm, human readable.

        Returns
        -------
        bool

        Examples
        --------
        >>> idadb._check_procedure('KMEANS')
        True
        >>> idadb._check_procedure('NOT_EXISTING')
        IdaDatabaseError: Function 'NOT_EXISTING' is not available.
        """

        if alg_name is None:
            alg_name = proc_name

        query = ("SELECT COUNT(*) FROM SYSCAT.ROUTINES WHERE ROUTINENAME='%s" +
                 "' AND ROUTINEMODULENAME = 'IDAX'") % proc_name
        flag = self.ida_scalar_query(query)

        if int(flag) == False:
            raise IdaDataBaseError("Function '%s' is not available." % alg_name)
        else:
            return True

    def _call_stored_procedure(self, sp_name, **kwargs):
        """
        Call a specific stored procedure from DashDB/DB2 and return its result.

        Parameters
        ----------
        sp_name : str
            Name of the stored procedure.
        **kwargs : ...
            Additional parameters, specific to the called stored procedure.
        """
        tmp = []
        views = []
        for key, value in six.iteritems(kwargs):
            if value is None:
                continue  # Go to next iteration
            if isinstance(value, ibmdbpy.frame.IdaDataFrame):
                tmp_view_name = self._create_view(value)
                tmp.append("%s=%s" % (key, tmp_view_name))
                views.append(tmp_view_name)
            elif isinstance(value, six.string_types) and all([x != " " for x in value]):
                if key == "intable":
                    tmp.append("%s=%s"% (key, value))  # no " in the case it is a table name
                else:
                    tmp.append("%s=\"%s\"" % (key, value))
            elif isinstance(value, list):
                tmp.append("%s=\"%s\"" % (key, " ".join(str(value))))
            else:
                tmp.append("%s=%s" % (key, value))
        try:
            call = "CALL %s('%s')" % (sp_name, ",".join(tmp))
            result = self._prepare_and_execute(call)
        except:
            query = "values idax.last_message"
            raise IdaDataBaseError(self.ida_scalar_query(query))
        finally:
            for view in views:
                self.drop_view(view)

        return result

    def _autocommit(self):
        """
        Commit changes made to the database in the connection automatically.
        If the environment variable 'AUTOCOMMIT' is set to True, then commit.

        Notes
        -----
        In the case of a commit operation, all changes that are made in the
        Database after the last commit, including those in the children
        IdaDataFrames, will be commited.

        If the environment variable 'VERBOSE' is not set to 'True', the
        autocommit operations will not be notified in the console to the user.
        """
        if os.getenv('AUTOCOMMIT') == 'True':
            self._con.commit()
            if os.getenv('VERBOSE') == 'True':
                print("<< AUTOCOMMIT >>")

    def _check_connection(self):
        """
        Check if the connection still exists by trying to open a cursor.
        """
        if self._con_type == "odbc":
            try:
                self._con.cursor()
            except Exception as e:
                raise IdaDataBaseError(e.value[-1])
        elif self._con_type == "jdbc":
            try:
                # Avoid infinite recursion
                sql.ida_query(self,"SELECT distinct TABSCHEMA, TABNAME, OWNER,"+
                              " TYPE from SYSCAT.TABLES WHERE (OWNERTYPE = 'U')",
                              True, True)
            except Exception as e:
                raise IdaDataBaseError("The connection is closed")

    def _retrieve_cache(self, cache):
        """
        Helper function that retrieve cache if available.
        Cache are just string type values stored in private attributes.
        """
        if not isinstance(cache, six.string_types):
            raise TypeError("cache is not of string type")

        self._check_connection()

        if hasattr(self, cache):
            return getattr(self,cache)
        else:
            return None

    def _reset_attributes(self, attributes):
        """
        Helper function that delete attributes given as parameter if they
        exists in self. This is used to refresh lazy attributes and caches.
        """
        ibmdbpy.utils._reset_attributes(self, attributes)
