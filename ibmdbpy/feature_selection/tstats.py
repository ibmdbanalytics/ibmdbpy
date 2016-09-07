# -*- coding: utf-8 -*-
"""
Created on Mon Dec 14 13:05:27 2015

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

import numpy as np 
import pandas as pd

#from ibmdbpy.internals import idadf_state
from ibmdbpy.feature_selection.private import _check_input

#@idadf_state#(force=True)
def ttest(idadf, target=None, features=None, ignore_indexer=True):
    """
    Compute the t-statistics values of a set of features against a set of 
    target attributes. 
    
    Parameters
    ----------
    idadf : IdaDataFrame
    
    target : str or list of str, optional
        A column or list of columns against which the t-statistcs values will 
        be computed. Per default, consider all columns
    
    features : str or list of str, optional
        A column or list of columns for which the t-statistics values will be 
        computed against each target attributes. Per default, consider all 
        columns, except non numerical columns. 
    
    ignore_indexer : bool, default: True
        Per default, ignore the column declared as indexer in idadf
        
    Returns
    -------
    Pandas.DataFrame or Pandas.Series if only one target
    
    Raises
    ------
    TypeError
        If the features argument or the data set does not contains any 
        numerical features. Raise TypeError. 
        
    Notes
    -----
    This implements the "modified" ttest as defined in the paper
    A Modified T-test feature Selection Method and Its Application on
    the HapMap Genotype Data (Zhou et al.)
    
    The target columns should be categorical, while the feature columns should
    be numerical.
    
    The scalability of this approach is not very good. Should not be used on 
    high dimensional data. 
    
    Examples
    --------
    >>> idadf = IdaDataFrame(idadb, "IRIS")
    >>> ttest(idadf,"CLASS")
    """
    # Check input
    target, features = _check_input(idadf, target, features, ignore_indexer)
    ttest_dict = OrderedDict()
    length = len(idadf)
    
    S_dict = dict()
    M_dict = dict()
    class_mean_dict = dict()
    
    numerical_columns = idadf._get_numerical_columns()
    
    # Filter out non numerical columns
    features = [feature for feature in features if feature in numerical_columns]
    if not features:
        raise TypeError("No numerical features.")
        
    #mean = idadf[features].mean() # This is broken
    mean = idadf.mean()
    
    if target is None:
        target = list(idadf.columns)
            
    for t in target:
        features_notarget = [x for x in features if (x != t)]
    
        if t not in M_dict:
            count = idadf.count_groupby(t)    
            target_count = count["count"]
            target_count.index = count[t]
            M_dict[t] = np.sqrt(1/target_count + 1/length)     
        
        if t not in S_dict:
            S_dict[t] = idadf.within_class_std(target = t, features = features_notarget)
        
        if t not in class_mean_dict:
            class_mean_dict[t] = idadf.mean_groupby(t, features = features_notarget)
            
        M = M_dict[t]
        S = S_dict[t]
        class_mean = class_mean_dict[t]
        
        ttest_dict[t] = OrderedDict()
        for feature in features_notarget:
            ttest_dict[t][feature] = OrderedDict()
            for target_class in class_mean.index:
                numerator = abs(class_mean.loc[target_class][feature] - mean[feature])
                denominator = M[target_class] * S[feature]
                
                ttest_dict[t][feature][target_class] = numerator / denominator
                    
        for feature in features_notarget:
            ttest_dict[t][feature] = max(ttest_dict[t][feature].values())
        
    result = pd.DataFrame(ttest_dict)
    
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