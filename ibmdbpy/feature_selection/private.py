# -*- coding: utf-8 -*-
"""
Created on Mon Nov 23 10:46:25 2015

@author: efouche
"""

import six

def _check_input(idadf, target, features, ignore_indexer=True):
    """
    Check if the input is valid, i.e. if each column in target and features
    exists in idadf. 
    
    Parameters
    ----------
    target: str or list of str
        A column or list of columns to be used as target
        
    features: str or list of str
        A column or list of columns to be used as feature
    
    ignore_indexer: bool, default: True
        If True, remove the indexer from the features set, as long as an
        indexer is defined in idadf
    """
    if target is not None:
        if isinstance(target, six.string_types):
            if target not in idadf.columns:
                raise ValueError("Unknown target column %s"%target)
            target = [target]
        else:
            for x in target:
                if x not in idadf.columns:
                    raise ValueError("Unknown target column %s"%x)
            
    if features is not None:
        if isinstance(features, six.string_types):
            if features not in idadf.columns:
                raise ValueError("Unknown feature column %s"%features)
            features = [features]
        else:
            for x in features:
                if x not in idadf.columns:
                    raise ValueError("Unknown feature column %s"%x)
        if target is None:
            if len(features) == 1:
                raise ValueError("Cannot compute correlation coefficients of only one"+
                                 " column (%s), need at least 2"%features[0])
    else:
        if target is not None:
            if len(target) == 1:
                features = [x for x in idadf.columns if x not in target]
            else:
                features = list(idadf.columns)
        else:
            features = list(idadf.columns)
            
        ## Remove indexer from feature list
        # This is useless and expensive to compute with a primary key 
        if ignore_indexer is True:
            if idadf.indexer:
                if idadf.indexer in features:
                    features.remove(idadf.indexer)
    
    # Catch the case where users ask for the correlation between the two same columns
    if target == features:
        if len(target) == 1:
            raise ValueError("The correlation value of two same columns is always maximal")
            
    return target, features