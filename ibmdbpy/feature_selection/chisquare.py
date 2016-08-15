# -*- coding: utf-8 -*-
"""
Created on Mon Dec 14 11:31:52 2015

@author: efouche
"""

from collections import OrderedDict

from ibmdbpy.internals import idadf_state
from ibmdbpy.utils import timed

import numpy as np 
import pandas as pd

from ibmdbpy.feature_selection.private import _check_input


@idadf_state
@timed
def chisquare(idadf, target = None, features = None, ignore_indexer=True):
    """
    Compute the chisquare statistics coefficients between a set of features 
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
    
    Chisquare as defined in 
    A Comparative Study on Feature Selection and Classification Methods Using 
    Gene Expression Profiles and Proteomic Patterns. (GIW02F006)
    
    The scalability of this approach is not very good. Should not be used on 
    high dimensional data. 
    
    Examples
    --------
    >>> idadf = IdaDataFrame(idadb, "IRIS")
    >>> chisquare(idadf)
    """
    # Check input
    if target is None:
        if features is None:
            target = list(idadf.columns) 
        else:
            target = features
            
    target, features = _check_input(idadf, target, features, ignore_indexer)
    count_dict = dict()
    length = len(idadf)
    
    values = OrderedDict()
         
    for t in target:   
        values[t] = OrderedDict() 
        features_notarget = [x for x in features if (x != t)]
        
        ### Compute
        for feature in features_notarget:
            ############
            target = t
            feature = feature
        
            if target not in count_dict:
                count = idadf.count_groupby(target)
                count_serie = count["count"]
                count_serie.index = count[target]
                count_dict[target] = count_serie
        
            C = dict(count_dict[target])
            
            if feature not in count_dict:
                count = idadf.count_groupby(feature)
                count_serie = count["count"]
                count_serie.index = count[feature]
                count_dict[feature] = count_serie
                
            R = dict(count_dict[feature])
            
            if (feature, target) not in count_dict:
                count_dict[(feature, target)] = idadf.count_groupby([feature , target])
            
            count = count_dict[(feature, target)]
            
            chi = 0            
            for target_class in C.keys():
                count_target = count[count[target] == target_class][[feature, "count"]]
                A_target = count_target['count']
                A_target.index = count_target[feature]
                
                for feature_class in A_target.index:
                    a = A_target[feature_class]
                    e = R[feature_class] * C[target_class] / length
                    chi += ((a - e)**2)/e
            
            values[t][feature] = chi
        

    result = pd.DataFrame(values).fillna(np.nan)
        
    if len(result.columns) == 1:
        if len(result) == 1:
            result = result.iloc[0,0]
        else:
            result = result[result.columns[0]].copy()
            result.sort(ascending = False) 
    else:
        order = [x for x in result.columns if x in features] + [x for x in features if x not in result.columns]
        result = result.reindex(order)

    return result
    
    
