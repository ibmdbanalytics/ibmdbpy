# -*- coding: utf-8 -*-
"""
Created on Mon Dec 14 11:31:52 2015

@author: efouche
"""
from __future__ import division
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import absolute_import
from builtins import dict
from future import standard_library
standard_library.install_aliases()

from collections import OrderedDict

from ibmdbpy.internals import idadf_state
from ibmdbpy.utils import timed

import numpy as np 
import pandas as pd

from ibmdbpy.feature_selection.private import _check_input

@idadf_state
@timed
def chisquared(idadf, target = None, features = None, ignore_indexer=True):
    """
    Compute the Chi-Squared statistics coefficients between a set of features 
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
    Input columns as target and features should be categorical, otherwise 
    this measure does not make much sense. 
    
    Chi-squared as defined in 
    A Comparative Study on Feature Selection and Classification Methods Using 
    Gene Expression Profiles and Proteomic Patterns. (GIW02F006)
    
    The scalability of this approach is not very good. Should not be used on 
    high dimensional data. 
    
    Examples
    --------
    >>> idadf = IdaDataFrame(idadb, "IRIS")
    >>> chisquared(idadf)
    """
    # Check input
    target, features = _check_input(idadf, target, features, ignore_indexer)
    count_dict = dict()
    length = len(idadf)
    
    values = OrderedDict()
         
    for t in target:   
        if t not in values:
            values[t] = OrderedDict() 
        features_notarget = [x for x in features if (x != t)]
        
        ### Compute
        for feature in features_notarget:
            if feature not in values:
                values[feature] = OrderedDict()
            if t not in values[feature]:
                if t not in count_dict:
                    count = idadf.count_groupby(t)
                    count_serie = count["count"]
                    count_serie.index = count[t]
                    count_dict[t] = count_serie
            
                C = dict(count_dict[t])
                
                if feature not in count_dict:
                    count = idadf.count_groupby(feature)
                    count_serie = count["count"]
                    count_serie.index = count[feature]
                    count_dict[feature] = count_serie
                    
                R = dict(count_dict[feature])
                
                if (feature, t) not in count_dict:
                    count_dict[(feature, t)] = idadf.count_groupby([feature , t])
                
                count = count_dict[(feature, t)]
                
                chi = 0            
                for target_class in C.keys():
                    count_target = count[count[t] == target_class][[feature, "count"]]
                    A_target = count_target['count']
                    A_target.index = count_target[feature]
                    
                    for feature_class in A_target.index:
                        a = A_target[feature_class]
                        e = R[feature_class] * C[target_class] / length
                        chi += ((a - e)**2)/e
                
                values[t][feature] = chi   # chisquared is symmetric 
                if feature in target:
                    values[feature][t] = chi
        
    result = pd.DataFrame(values).fillna(np.nan)
    result = result.dropna(axis=1, how="all")
        
    if len(result.columns) > 1:
        order = [x for x in result.columns if x in features] + [x for x in features if x not in result.columns]
        result = result.reindex(order)
    
    if len(result.columns) == 1:
        if len(result) == 1:
            result = result.iloc[0,0]
        else:
            result = result[result.columns[0]].copy()
            result.sort(ascending = False) 
        

    
    
    return result
    
    
