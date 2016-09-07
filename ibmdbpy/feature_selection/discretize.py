# -*- coding: utf-8 -*-
"""
Created on Mon Nov 23 09:02:30 2015

@author: efouche
"""
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()

from ibmdbpy.internals import idadf_state
import ibmdbpy
from ibmdbpy.utils import timed

import six

@timed
@idadf_state(force = True)   
def discretize(idadf, columns = None, disc= "em", target = None, bins = None, outtable = None, clear_existing=False):
    """
    Discretize a set of numerical columns from an IdaDataFrame and returns an 
    IdaDataFrame open on the discretized version of the dataset. 
    
    Parameters
    ----------
    idadf : IdaDataFrame
    
    columns : str or list of str, optional
        A column or list of columns to be discretized
    
    disc : "ef", "em", "ew", "ewn" default: "em"
        Discretization method to be used
        
        - ef: Discretization bins of equal frequency 
        
        - em: Discretization bins of minimal entropy 
        
        - ew: Discretization bins of equal width
        
        - ewn: Discretization bins of equal width with human-friendly limits 
    
    target : str
        Target column again which the discretization will be done. Relevant
        only for "em" discretization. 
        
    bins: int, optional
        Number of bins. Not relevant for "em" discretization. 
        
    outtable: str, optional
        The name of the output table where the assigned clusters are stored.
        If this parameter is not specified, it is generated automatically.
        If the parameter corresponds to an existing table in the database,
        it is replaced.
    
    clear_existing: bool, default: False
        If set to True, a table will be replaced when a table with the same 
        name already exists  in the database.
    """
    if columns is None:
        columns = idadf._get_numerical_columns()
        if target is not None:
            columns = [x for x in columns if columns != target]    
    else:
        if isinstance(columns, six.string_types):
                columns = [columns]
                
    stored_proc = _check(idadf, columns, disc, target, bins, outtable)

    bound_outtable = idadf._idadb._get_valid_tablename('DISC_BOUNDS_%s_'%idadf.tablename)
    intable = idadf.name   # either the table or a view on the top 
    incolumn = "\";\"".join(columns)
    
    
    # Calculate bounds
    idadf._idadb._call_stored_procedure("IDAX.%s"%stored_proc,
                                        outtable=bound_outtable,
                                        intable=intable,
                                        incolumn=incolumn,
                                        target=target,
                                        bins=bins)
        
    # Create discretized dataset
        
    if outtable is None:
        disc_outtable = idadf._idadb._get_valid_tablename('DISC_%s_'%idadf.tablename)
    else:
        if clear_existing is True:
            try:
                idadf._idadb.drop_table(outtable)
            except:
                pass
        disc_outtable = outtable
    
    try:
        idadf._idadb._call_stored_procedure("IDAX.APPLY_DISC",
                                            outtable=disc_outtable,
                                            intable=intable,
                                            btable=bound_outtable,
                                            replace="T")
    except:
        raise
    finally:
        idadf._idadb.drop_table(bound_outtable)
    
    return ibmdbpy.IdaDataFrame(idadf._idadb, disc_outtable)
    
    
def _check(idadf, columns, disc, target, bins, outtable):
    """
    Helper function to handle basic checks for 
    ibmdbpy.feature_selection.discretize
    """
    if outtable is not None:
        ibmdbpy.utils.check_tablename(outtable)
    if bins is not None:
        if not isinstance(bins, int):
            raise TypeError("bins argument is not of integer type")
            
    if columns is not None:
        
        if target is not None:
            if target in columns:
                raise ValueError("Target in columns.")
        unknown = []
        for column in columns:
            if column not in idadf.columns:
                unknown.append(column)
        if unknown:
            raise ValueError("Undefined columns: %s"%", ".join(unknown))
        
    if disc == "em":
        if bins is not None:
            raise ValueError("Number of bins is automatically detected for Entropy Minimization discretization.")
        if target is None:
            raise ValueError("Need to define a target for Entropy Minimization discretization.")
        if target in columns:
            raise ValueError("Target column %s cannot be discretize too"%target)
        if target not in idadf.columns:
            raise ValueError("Undefined target column %s"%target)
        stored_proc = "EMDISC"
    else:
        if target is not None:
            raise ValueError("Target attribute defined only for Entropy Minimization discretization.")
        if bins is None:
            bins = 10      
        if disc == "ef":
            stored_proc = "EFDISC"
        elif disc == "ew":
            stored_proc = "EWDISC"
        elif disc == "ewn":
            stored_proc = "EWDISC_NICE"
        else:
            raise ValueError("Unknown discretization method.")
            
    return stored_proc