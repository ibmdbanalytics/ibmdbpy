# -*- coding: utf-8 -*-
"""
Created on Mon Nov 23 09:05:39 2015

@author: efouche
"""
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from ibmdbpy4nps.internals import idadf_state
from ibmdbpy4nps.utils import timed
from collections import OrderedDict

import pandas as pd

import six

@idadf_state
def entropy(idadf, target=None, mode="normal", execute=True, ignore_indexer=True):
    """
    Compute the entropy for a set of features in an IdaDataFrame. 
    
    Parameters
    ----------
    idadf: IdaDataFrame
    
    target: str or list of str, optional
        A column or list of columns to be used as features. Per default, 
        consider all columns. 
    
    mode: "normal" or "raw"
        Experimental
        
    execute: bool, default:True
        Experimental. Execute the request or return the correponding SQL query 
    
    ignore_indexer: bool, default: True
        Per default, ignore the column declared as indexer in idadf
        
    Returns
    -------
    Pandas.Series
    
    Notes
    -----
    Input column should be categorical, otherwise this measure does not make 
    much sense. 
    
    Examples
    --------
    >>> idadf = IdaDataFrame(idadb, "IRIS")
    >>> entropy(idadf)
    """
    if target is not None:
        if isinstance(target, six.string_types):
            target = [target]
            
        targetstr = "\",\"".join(target)
        subquery = "SELECT COUNT(*) AS a FROM %s GROUP BY \"%s\""%(idadf.name,targetstr)
        if mode == "normal":
            length = len(idadf)
            query = "SELECT (SUM(-a*LOG(a))/%s+LOG(%s))/LOG(2) FROM (%s) AS T"%(length, length, subquery)
        elif mode == "raw":
            query = "SELECT SUM(-a*LOG(a)) FROM(%s) AS T "%(subquery)
        
        if not execute:
            query = query[:query.find("FROM")] + ",'%s'"%"\',\'".join(target) + query[query.find("FROM"):]
            return query
        return idadf.ida_scalar_query(query)
    else:
        entropy_dict = OrderedDict()
        columns = list(idadf.columns)
        # Remove indexer
        if ignore_indexer:
            if idadf.indexer:
                if idadf.indexer in columns:
                    columns.remove(idadf.indexer)
                    
        for column in columns:
           entropy_dict[column] = entropy(idadf, column, mode = mode)
                    
        # Output
        if len(columns) > 1:
            result = pd.Series(entropy_dict)
            result.sort_values(ascending = False)
        else:
            result = entropy_dict[columns[0]]
        return result
    
def entropy_stats(idadf, target=None, mode="normal", execute = True, ignore_indexer=True):
    """
    Similar to ibmdbby.feature_selection.entropy.entrop but use DB2 statistics
    to speed the computation. Returns an approximate value. Experimental. 
    
    Parameters
    ----------
    idadf: IdaDataFrame
    
    target: str or list of str, optional
        A column or list of columns to be used as features. Per default, 
        consider all columns. 
    
    mode: "normal" or "raw"
        Experimental
        
    execute: bool, default:True
        Experimental. Execute the request or return the correponding SQL query 
    
    ignore_indexer: bool, default: True
        Per default, ignore the column declared as indexer in idadf
        
    Returns
    -------
    Pandas.Series
    
    Notes
    -----
    Input column should be categorical, otherwise this measure does not make 
    much sense. 
    
    Cannot handle columns that are not physically existing in the database, 
    since no statistics are available for them. 
    
    Examples
    --------
    >>> idadf = IdaDataFrame(idadb, "IRIS")
    >>> entropy_stats(idadf)
    """
    if target is not None:
        subquery = "SELECT VALCOUNT as a FROM SYSCAT.COLDIST WHERE TABSCHEMA='%s' AND TABNAME = '%s' AND COLNAME='%s' AND TYPE='F' AND COLVALUE IS NOT NULL"%(idadf.schema, idadf.tablename, target)
        if mode == "normal":
            query = "SELECT(SUM(-a*LOG(a))/SUM(a)+LOG(SUM(a)))/LOG(2)FROM(%s)"%(subquery)
        elif mode == "raw":
            query = "SELECT SUM(-a*LOG(a)) FROM(%s)"%(subquery)
        
        if not execute:
            return query
        return idadf.ida_scalar_query(query)
    else:
        entropy_dict = OrderedDict()
        columns = list(idadf.columns)
        # Remove indexer
        if ignore_indexer:
            if idadf.indexer:
                if idadf.indexer in columns:
                    columns.remove(idadf.indexer)
                    
        for column in columns:
           entropy_dict[column] = entropy_stats(idadf, column, mode = mode)
                    
        # Output
        if len(columns) > 1:
            result = pd.Series(entropy_dict)
            result.sort_values(ascending = False)
        else:
            result = entropy_dict[columns[0]]
        return result