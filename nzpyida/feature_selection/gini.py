# -*- coding: utf-8 -*-
"""
Created on Tue Dec  1 12:29:30 2015

@author: efouche
"""
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()


from collections import OrderedDict

import pandas as pd
import numpy as np
import six

from nzpyida.internals import idadf_state
from nzpyida.utils import timed
from nzpyida.feature_selection.private import _check_input

@idadf_state
@timed
def gini_pairwise(idadf, target=None, features=None, ignore_indexer=True):
    """
    Compute the conditional gini coefficients between a set of features and a 
    set of target in an IdaDataFrame. 
    
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
    Input columns as target and features should be categorical, otherwise 
    this measure does not make much sense. 
    
    Examples
    --------
    >>> idadf = IdaDataFrame(idadb, "IRIS")
    >>> gini_pairwise(idadf)
    """
    # Check input
    target, features = _check_input(idadf, target, features, ignore_indexer)
        
    gini_dict = OrderedDict()
    length = len(idadf)

    if idadf._idadb._is_netezza_system():
        power_function = "POW"
        div_term = "* POW(c,-1)) * POW(%s,-1)"%length
    else:
        power_function = "POWER"
        div_term = "/c)/%s"%length

    for t in target:
        gini_dict[t] = OrderedDict() 
        features_notarget = [x for x in features if (x != t)]
        
        for feature in features_notarget:
            if t not in gini_dict:
                gini_dict[t] = OrderedDict()
            
            query = ("SELECT SUM((%s(c,2) - gini)%s FROM "+
            "(SELECT SUM(%s(count,2)) as gini, SUM(count) as c FROM "+
            "(SELECT CAST(COUNT(*) AS FLOAT) AS count, \"%s\" FROM %s GROUP BY \"%s\",\"%s\") AS T1 "+
            "GROUP BY \"%s\") AS T2 ")
            query0 = query%(power_function, div_term, power_function, feature, idadf.name, t, feature, feature)
            gini_dict[t][feature] = idadf.ida_scalar_query(query0)
            
    result = pd.DataFrame(gini_dict).fillna(np.nan)
        
    if len(result.columns) > 1:
        order = [x for x in result.columns if x in features] + [x for x in features if x not in result.columns]
        result = result.reindex(order)
       
    result = result.dropna(axis=1, how="all")
    
    if len(result.columns) == 1:
        if len(result) == 1:
            result = result.iloc[0,0]
        else:
            result = result[result.columns[0]].copy()
            result.sort_values(ascending = True)
    else:
        result = result.fillna(0)
    
    return result
               
    
        
    
    
    
@idadf_state
@timed
def gini(idadf, features=None, ignore_indexer=True):
    """
    Compute the gini coefficients for a set of features in an IdaDataFrame. 
    
    Parameters
    ----------
    idadf : IdaDataFrame
    
    features : str or list of str, optional
        A column or list of columns to be used as features. Per default, 
        consider all columns. 
    
    ignore_indexer : bool, default: True
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
    >>> gini(idadf)
    """
    if features is None:
        features = list(idadf.columns)
    else:
        if isinstance(features, six.string_types):
            features = [features]

    if ignore_indexer is True:
        if idadf.indexer:
            if idadf.indexer in features:
                features.remove(idadf.indexer)
      
        
    value_dict = OrderedDict()
        
    length = len(idadf)**2

    if idadf._idadb._is_netezza_system():
      power_function = "POW"
      div_term = "* POW(%s, -1)"%length
    else:
      power_function = "POWER"
      div_term ="/%s"%length

    for feature in features:
        
        subquery = "SELECT COUNT(*) AS count FROM %s GROUP BY \"%s\""%(idadf.name, feature)
        query = "SELECT (%s - SUM(%s(count,2)))%s FROM (%s) AS T "%(length, power_function, div_term, subquery)
        value_dict[feature] = idadf.ida_scalar_query(query)
            
        if len(features) > 1:
            result = pd.Series(value_dict) 
        else:
            result = value_dict[feature]
    
    return result