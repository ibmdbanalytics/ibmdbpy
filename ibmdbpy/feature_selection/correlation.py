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

from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from collections import OrderedDict

import ibmdbpy
from ibmdbpy.internals import idadf_state
from ibmdbpy.utils import timed, chunklist

import pandas as pd

from ibmdbpy.feature_selection.private import _check_input


@idadf_state
@timed
def pearson(idadf, target=None, features=None, ignore_indexer=True):
    """
    Compute the pearson correlation coefficients between a set of features and a 
    set of target in an IdaDataFrame. Provide more granualirity than 
    IdaDataFrame.corr
    
    Parameters
    ----------
    idadf : IdaDataFrame
    
    target : str or list of str, optional
        A column or list of columns against to be used as target. Per default, 
        consider all columns
    
    features : str or list of str, optional
        A column or list of columns to be used as features. Per default, 
        consider all columns. 
        
    ignore_indexer : bool, default: True
        Per default, ignore the column declared as indexer in idadf
        
    Returns
    -------
    Pandas.DataFrame or Pandas.Series if only one target
    
    Notes
    -----
    Input columns as target and features should be numerical. 
    
    Examples
    --------
    >>> idadf = IdaDataFrame(idadb, "IRIS")
    >>> pearson(idadf)
    """
    numerical_columns = idadf._get_numerical_columns()
    if features is None:
        features = numerical_columns
        
    target, features = _check_input(idadf, target, features, ignore_indexer)
    
    value_dict = OrderedDict()
    
    for feature in features:
        if feature not in numerical_columns:
            raise TypeError("Correlation-based measure not available for non-numerical column %s"%feature)
                    
    if target == features:
        return idadf.corr(features = features, ignore_indexer=ignore_indexer)
    else:
        for t in target:
            if feature not in numerical_columns:
                raise TypeError("Correlation-based measure not available for non-numerical column %s"%t)
        
        for t in target:
            value_dict[t] = OrderedDict()
            
            features_notarget = [x for x in features if x != t]
            
            if len(features_notarget) < 64:
                agg_list = ["CORRELATION(\"%s\",\"%s\")"%(x, t) for x in features_notarget]
                agg_string = ', '.join(agg_list)
                name = idadf.internal_state.current_state
                data = idadf.ida_query("SELECT %s FROM %s"%(agg_string, name), first_row_only = True)
            else:
                chunkgen = chunklist(features_notarget, 100)
                data = ()
                for chunk in chunkgen: 
                    agg_list = ["CORRELATION(\"%s\",\"%s\")"%(x, t) for x in chunk]
                    agg_string = ', '.join(agg_list)
            
                    name = idadf.internal_state.current_state
                    data += idadf.ida_query("SELECT %s FROM %s"%(agg_string, name), first_row_only = True)
    
            for i, feature in enumerate(features_notarget):
                value_dict[t][feature] = data[i]
        
        ### Fill the matrix
        result = pd.DataFrame(value_dict).fillna(1)
        
        if len(result.columns) == 1:
            if len(result) == 1:
                result = result.iloc[0,0]
            else:
                result = result[result.columns[0]].copy()
                result.sort_values(inplace=True, ascending=False)
        else:
            order = [x for x in result.columns if x in features] + [x for x in features if x not in result.columns]
            result = result.reindex(order)
        
        return result 
  
@idadf_state
@timed          
def spearman(idadf, target=None, features = None, ignore_indexer=True):
    """
    Compute the spearman rho correlation coefficients between a set of features 
    and a set of target in an IdaDataFrame.
    
    Parameters
    ----------
    idadf : IdaDataFrame
    
    target : str or list of str, optional
        A column or list of columns against to be used as target. Per default, 
        consider all columns
    
    features : str or list of str, optional
        A column or list of columns to be used as features. Per default, 
        consider all columns. 
        
    ignore_indexer : bool, default: True
        Per default, ignore the column declared as indexer in idadf
        
    Returns
    -------
    Pandas.DataFrame or Pandas.Series if only one target
    
    Notes
    -----
    Input columns as target and features should be numerical. 
    This function is a wrapper for pearson. 
    The scalability of this approach is not very good. Should not be used on 
    high dimensional data. 
    
    Examples
    --------
    >>> idadf = IdaDataFrame(idadb, "IRIS")
    >>> spearman(idadf)
    """
    numerical_columns = idadf._get_numerical_columns()
    if features is None:
        features = numerical_columns
        
    target, features = _check_input(idadf, target, features, ignore_indexer)
    
    for feature in features:
        if feature not in numerical_columns:
            raise TypeError("Correlation-based measure not available for non-numerical column %s"%feature)
    
    if ignore_indexer is True:
        if idadf.indexer:
            if idadf.indexer in numerical_columns:
                features.remove(idadf.indexer)
    
    if features is None:
        features = list(idadf.columns)
    
    numerical_features = [x for x in features if x in numerical_columns]
    numerical_targets = [x for x in target if x in numerical_columns]
    
    numerical_features = list(set(numerical_features) | set(numerical_targets))
    
    
    agg_list = ["CAST(RANK() OVER (ORDER BY \"%s\") AS INTEGER) AS \"%s\""%(x, x) for x in numerical_features]
    agg_string = ', '.join(agg_list)
    
    expression = "SELECT %s FROM %s"%(agg_string, idadf.name)
    
    viewname = idadf._idadb._create_view_from_expression(expression)
    
    try:
        idadf_rank = ibmdbpy.IdaDataFrame(idadf._idadb, viewname)
        return pearson(idadf_rank, target = target, features=numerical_features, ignore_indexer=ignore_indexer)
    except:
        raise
    finally:
        idadf._idadb.drop_view(viewname)
    
    
    
    
 
        
        

        