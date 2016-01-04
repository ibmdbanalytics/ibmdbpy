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

""" Utility functions """

# Python 2 compatibility
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()

import decimal
import numpy as np
import os

def _prepare_query(query_string, silent = False):
    """
    Return a formatted query string and print query if verbose mode activated

    Parameters
    ----------
    query_string : str
        String to be printed
    silent : bool, default: False
        If True, the query will not be printed at all

    Returns
    -------
    querystring : str
    """
    if silent is False:
        if os.getenv('VERBOSE') == 'True':
            print("> " + query_string)
    return query_string

def _prepare_and_execute(idaobject, query, autocommit = True, silent = False):
    """
    See IdaDataBase._prepare_and_execute
    """
    # Open a cursor
    cursor = idaobject._con.cursor()

    try:
        query = _prepare_query(query, silent)
        #print(query)
        cursor.execute(query)
        if autocommit is True:
            idaobject._autocommit()
    except:
        raise
    else:
        return True
    finally:
        cursor.close()


def ida_query(idadb, query, silent=False, first_row_only=False, autocommit = False):
    """
    See IdaDataBase.ida_query
    """
    # Open a cursor
    cursor = idadb._con.cursor()

    try:
        query = _prepare_query(query, silent)
        cursor.execute(query)
        
        if autocommit is True:
            idadb._autocommit()
        
        if first_row_only is True:
            try:
                result = cursor.fetchone()
            except:
                pass  # The SQL command does not give anything back. 
            else:
                tuple_as_list = list(tuple(result)) # Tuples are immutable
                for index, element in enumerate(tuple_as_list):
                    if element is None:
                        tuple_as_list[index] = np.nan
                    if isinstance(element, decimal.Decimal):
                        tuple_as_list[index] = int(element)
                result = tuple(tuple_as_list)
                return result
        else:
            from pandas.io.sql import read_sql
            try:
                result = read_sql(query, idadb._con)
            except:
                pass  # The SQL command does not give anything back. 
            else:
                if len(result.columns) == 1:
                    result = result[result.columns[0]]
                return result 
    except:
        raise
    finally:
        cursor.close()
    #return result

def ida_scalar_query(idadb, query, silent = False, autocommit = False):
    """
    See IdaDataBase.ida_scalar_query
    """
    # Open a cursor
    cursor = idadb._con.cursor()

    try:
        query = _prepare_query(query, silent)
        cursor.execute(query)
        
        if autocommit is True:
            idadb._autocommit()
            
        result = cursor.fetchone()[0]
        if result is None:
            result = np.nan
    except:
        raise
    finally:
        cursor.close()
    return result