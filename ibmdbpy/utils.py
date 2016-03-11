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

""" Utility functions """

# Python 2 compatibility
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from builtins import input
from future import standard_library
standard_library.install_aliases()

import sys
import os
import warnings
from time import time
from functools import wraps

import six

#-----------------------------------------------------------------------------
# Environment variable setter

def set_verbose(is_verbose):
    """
    Set the environment variable “VERBOSE” to ‘TRUE’ or ‘FALSE’. If it is set 
    to ‘TRUE’, all SQL request are printed in the console.
    """
    if is_verbose is True:
        os.environ['VERBOSE'] = 'True'
    elif is_verbose is False:
        os.environ['VERBOSE'] = 'False'
    else:
        raise ValueError("is_verbose should be a boolean")

def set_autocommit(is_autocommit):
    """
    Set the environment variable “AUTCOMMIT” to ‘TRUE’ or ‘FALSE’. If it is set 
    to ‘TRUE’, all operations are committed automatically.
    """
    if is_autocommit is True:
        os.environ['AUTOCOMMIT'] = 'True'
    elif is_autocommit is False:
        os.environ['AUTOCOMMIT'] = 'False'
    else:
        raise ValueError("is_autocommit should be a boolean")

#-----------------------------------------------------------------------
# Performance measurement wrapper

def timed(function):
    """
    Measure the elapsed time of custom functions of the package. Should be used 
    as a decorator.
    """
    @wraps(function)
    def wrapper(*args, **kwds):
        """Calculate elapsed time in seconds"""
        start = time()
        result = function(*args, **kwds)
        elapsed = time() - start
        if os.environ['VERBOSE'] == 'True':
            print("Execution time: %s seconds." %elapsed)
        return result
    return wrapper

def query_yes_no(question, default=None):
    """
    Ask a yes/no question via raw_input() and return its answer.

    Parameters
    ----------
    question : str
        Question to be asked to the user. Should be a yes/no question.
    default : "yes"/"no"
        The presumed answer if the user just hits <Enter>.

    Returns
    -------
    bool
    """
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower() # in Python 2.7, it is raw_input
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")

def check_tablename(tablename):
    """
    Check if a string is upper case. This function converts the string to upper 
    case and checks if it is a valid table name.

    Parameters
    ----------
    tablename : str
        string to be checked. 

    Returns
    -------
    str
        Checked and upper cased table name.

    Notes
    -----
        Table names should consist of upper case characters or numbers that can 
        be separated by underscores (“_”) characters.
    """
    tablename = check_case(tablename)
    if not all([(char.isalnum() | (char == '_') | (char == '.')) for char in tablename]):
        raise ValueError("Table name is not valid, only alphanumeric characters and underscores are allowed.")
    if tablename.count(".") > 1:
        raise ValueError("Table name is not valid, only one '.' character is allowed.")
    return tablename

def check_viewname(viewname):
    """
    Convenience function. Just an alternative name to check_tablename for 
    checking view names, which have the same prerequisites as tablenames. See 
    the check_tablename documentation.
    """
    return check_tablename(viewname)

def check_case(name):
    """
    Check if the name given as parameter is in upper case and convert it to 
    upper cases.
    """
    if name != name.upper():
        warnings.warn("Mixed case names are not supported in database object names.", UserWarning)
    return name.upper()

def _convert_dtypes(idadf, data):
    """
    DEPRECATED - CURRENTLY NOT IN USE
    was used for formatting dataframe types

    Convert datatypes in a dataframe to float or to int according to the 
    corresponding type in database. This function only works if the dataframe 
    that is given as a parameter has the same columns as the current 
    IdaDataFrame.
    """
    # Note : Here I made the choice to convert every numeric attributes to
    # float, the reason is that converting to int for an attribute that has
    # missing values leads to an error, this is known as a pandas' gotcha
    # We could use statistics._get_number_of_nas to know if there are
    # missing values for each attributes, but I made the choice no to do it,
    # this for reducing complexity and improve performance. However, it no
    # efficient in terms of memory.
    import pdb ; pdb.set_trace() ;
    original_dtype = idadf.dtypes

    int_attributes = ['SMALLINT', 'INTEGER', 'BIGINT', 'BOOLEAN']
    float_attributes = ['REAL', 'DOUBLE', 'FLOAT', 'DECIMAL',
                        'DECFLOAT', 'APPROXIMATE', 'NUMERIC']
    for index, dtype in enumerate(original_dtype.values):
        if dtype[0] in int_attributes + float_attributes:
            data[[data.columns[index]]] = data[[data.columns[index]]].astype(float)
        else:
            data[[data.columns[index]]] = data[[data.columns[index]]].astype(object)
    return data

def _reset_attributes(idaobject, attributes):
    """
    Delete an attribute of a list of attributes of an object, if the attribute exists.
    """
    if (not hasattr(attributes, "__iter__"))|isinstance(attributes, six.string_types):
        attributes = [attributes]
    for attribute in attributes:
        try:
            delattr(idaobject, attribute)
        except AttributeError:
            pass
        
def _check_input(idadf, target, features):
    """
    Check if the input is valid. 
    Target should be a string that exists as a column in the IdaDataFrame, or None.
    Features should a string or a list of columns in the IdaDataFrame, or None.
    If features is None, a list with all features different from target is returned
    """
    if target is not None:
        if not isinstance(target, six.string_types):
            raise ValueError("target should be a string")
        if target not in idadf.columns:
            raise ValueError("Unknown column %s"%target)
    
    if features is not None:
        if isinstance(features, six.string_types):
            if features not in idadf.columns:
                raise ValueError("Unknown column %s"%features)
            features = [features]
        for x in features:
            if x not in idadf.columns:
                raise ValueError("Unknown column %s"%x)
    else:
        if target is not None:
            features = [x for x in idadf.columns if x not in target]
        else:
            features = list(idadf.columns)
            
    return target, features
