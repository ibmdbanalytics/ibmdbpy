# -*- coding: utf-8 -*-
"""
Created on Mon Nov 23 09:48:18 2015

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

from ibmdbpy.feature_selection.entropy import entropy 

from ibmdbpy.internals import idadf_state
from ibmdbpy.utils import timed

import numpy as np 
import pandas as pd

from ibmdbpy.feature_selection.private import _check_input


@idadf_state
@timed
def gain_ratio(idadf, target = None, features = None, symmetry=True, ignore_indexer=True):
    """
    Compute the gain ratio coefficients between a set of features and a 
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
        
    symmetry : bool, default: True
        If True, compute the symmetric gain ratio as defined by
        [Lopez de Mantaras 1991]. Otherwise, the asymmetric gain ratio. 
    
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
    >>> gain_ratio(idadf)
    """
    # Check input 
    target, features = _check_input(idadf, target, features, ignore_indexer)
    
    entropy_dict = dict()
    length = len(idadf)
    values = OrderedDict()
    corrector = length*np.log(length)
        
    for t in target:
        if t not in values:
            values[t] = OrderedDict()
        features_notarget = [x for x in features if (x != t)]
        
        for feature in features_notarget:
            if feature not in values:
                values[feature] = OrderedDict()      
                
            if t not in values[feature]:    # i.e. it was not already computed 
                if t not in entropy_dict:
                    entropy_dict[t] = entropy(idadf, t, mode = "raw")
                if feature not in entropy_dict:
                    entropy_dict[feature] = entropy(idadf, feature, mode = "raw")
                    
                join_entropy = entropy(idadf,  [t] + [feature], mode = "raw")     
                disjoin_entropy = entropy_dict[t] + entropy_dict[feature]
                info_gain = (disjoin_entropy - join_entropy)
            
                if symmetry:
                    gain_ratio = (info_gain + corrector)/(disjoin_entropy + 2*corrector) # 2* because symmetric
                    values[t][feature] = gain_ratio
                    if feature in target:
                        values[feature][t] = gain_ratio
                else:
                    gain_ratio_1 = (info_gain + corrector)/(entropy_dict[t] + corrector)
                    values[t][feature] = gain_ratio_1
                    if feature in target:
                        gain_ratio_2 = (info_gain + corrector)/(entropy_dict[feature] + corrector)
                        values[feature][t] = gain_ratio_2
             
    ### Fill the matrix
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
            result.sort(ascending = True) 
    else:
        result = result.fillna(1)
            
    return result