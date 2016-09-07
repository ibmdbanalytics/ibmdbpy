# -*- coding: utf-8 -*-
"""
Created on Mon Nov 23 09:53:19 2015

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

import ibmdbpy

from ibmdbpy.feature_selection import entropy
from ibmdbpy.feature_selection.private import _check_input

from ibmdbpy.internals import idadf_state
from ibmdbpy.utils import timed

import pandas as pd
import numpy as np

@idadf_state
@timed
def su(idadf, target = None, features = None, ignore_indexer=True):
    """
    Compute the symmetric uncertainty coefficients between a set of features
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
    
    Examples
    --------
    >>> idadf = IdaDataFrame(idadb, "IRIS")
    >>> su(idadf)
    """
    # Check input
    target, features = _check_input(idadf, target, features, ignore_indexer)
                
    entropy_dict = dict()
    length = len(idadf)
    corrector = np.log(length)*length
    values = OrderedDict()
        
    for t in target:
        if t not in values:
            values[t] = OrderedDict() 
        features_notarget = [x for x in features if (x != t)]
        
        for feature in features_notarget:
            if feature not in values:
                values[feature] = OrderedDict()
            if t not in values[feature]:
                if t not in entropy_dict:
                    entropy_dict[t] = entropy(idadf, t, mode = "raw")
                if feature not in entropy_dict:
                    entropy_dict[feature] = entropy(idadf, feature, mode = "raw")
                join_entropy = entropy(idadf, [t] + [feature], mode = "raw")     
                disjoin_entropy = entropy_dict[t] + entropy_dict[feature]
                value = (2.0*(disjoin_entropy - join_entropy + corrector)/(disjoin_entropy + corrector*2))
                values[t][feature] = value
                if feature in target:
                    values[feature][t] = value
    
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

@idadf_state
@timed
def outer_su(idadf1, key1, idadf2, key2, target = None, features1 = None, features2 = None):
    """
    Compute the symmetric uncertainty coefficients between a set of features
    and a set of target from two different IdaDataFrames on a particular key. 
    
    This is experimental 
    """
    target1, features1 = _check_input(idadf1, target, features1)
    target2, features2 = _check_input(idadf2, None, features2)
    
    if key1 not in idadf1.columns:
        raise ValueError("%s is not a column in idadf1")
    if key2 not in idadf2.columns:
        raise ValueError("%s is not a column in idadf2")
       
    condition = "a.\"%s\" = b.\"%s\""%(key1,key2)
    
    if key2 in features2:
        features2.remove(key2)
    
    afeaturesas = ", ".join(["a.\"%s\" as \"a.%s\" "%(feature, feature) for feature in features1])
    bfeaturesas = ", ".join(["b.\"%s\" as \"b.%s\" "%(feature, feature) for feature in features2])
    
    selectlist = [afeaturesas, bfeaturesas]
    
    if target1 is not None:
        atargetas = ", ".join(["a.\"%s\" as \"a.%s\" "%(tar, tar) for tar in [target1]])
        selectlist.append(atargetas)
        atarget = "a." + target1
    else:
        atarget = None
        
    abfeatures = ["a." + feature for feature in features1] + ["b." + feature for feature in features2]
    selectstr = ", ".join(selectlist)
    
    expression = "SELECT %s FROM %s as a FULL OUTER JOIN %s as b ON %s"%(selectstr, idadf1.name, idadf2.name, condition)
    
    viewname = idadf1._idadb._create_view_from_expression(expression)
    
    try:
        idadf_join = ibmdbpy.IdaDataFrame(idadf1._idadb, viewname)
        return su(idadf_join, target = atarget, features = abfeatures)
    except:
        raise
    finally:
        idadf1._idadb.drop_view(viewname)