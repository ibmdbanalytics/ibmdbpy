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

"""The module implement objects and functions that are used to filter IdaDataFrame objects"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from builtins import str
from future import standard_library
standard_library.install_aliases()

from copy import deepcopy
from numbers import Number
from ibmdbpy.exceptions import IdaDataBaseError
import six

class FilterQuery(object):
    """
    FilterQueries are used to represent the filtering of an IdaDataFrame
    in ibmdbpy. The use of comparison operators, such as <, <=, ==, >=, >
    on an IdaDataFrame instance produces a FilterQuery instance which acts as a
    container for the where clause of the corresponding SQL request.

    Filtering is possible using a Pandas-like syntax. Applying comparison
    operators to IdaDataFrames produces a FilterQuery instance which
    contains the string embedding the corresponding where clause in the 
    “wherestr” attribute.

    FilterQuery objects also contain a logic that allows them to be
    combined, thus allowing complex filtering.

      You can combine the following operators: |, &, ^ (OR, AND and XOR)

    Examples
    --------
    >>> idadf[['sepal_length', 'petal_width'] < 5
    >>> <ibmdbpy.filtering.FilterQuery at 0xa65ba90>
    >>> _.wherestr
    '("sepal_length" < 5 AND "petal_width" < 5)'

    >>> idadf[idadf[['sepal_length', 'petal_width']] < 5]
    <ibmdbpy.frame.IdaDataFrame at 0xa73a860>
    >>> _.head() # filtered IdaDataFrame
        sepal_length  sepal_width  petal_length  petal_width     species
    0           4.4          2.9           1.4          0.2      setosa
    1           4.7          3.2           1.6          0.2      setosa
    2           4.9          2.5           4.5          1.7   virginica
    3           4.9          2.4           3.3          1.0  versicolor
    4           4.6          3.2           1.4          0.2      setosa

    >>> idadf[(idadf['sepal_length'] < 5) & (idadf[petal_width'] > 1.5)]
    <ibmdbpy.frame.IdaDataFrame at 0xa74b9b0>
    >>> _.head()
       sepal_length  sepal_width  petal_length  petal_width    species
    0           4.9          2.5           4.5          1.7  virginica

    Notes
    -----
    It is not possible to filter an IdaDataFrame by using an IdaDataFrame that 
    is opened in a different data source in the database. This is due to the 
    fact that, using a Pandas-like syntax, “idadf[‘petal_width’] < 5” will 
    return a Boolean array that is used to subset the original DataFrame. This 
    is a fundamental restriction of ibmdbpy: we cannot afford to compute and 
    download such an array because we cannot assume that the result will fit 
    into user’s memory. Download time can also be a performance issue.

    """
    def __init__(self, columns, tablename, method, value):
        """
        Constructor for filterquery objects.

        Parameters
        ----------
        columns : str or list
            columns on which the filter operation should be applied
        tablename : str
            Name of the table to which columns belong
        method : str
            value representing the comparision operator.
            Admissible value:  ["lt","le","eq","ne","ge,"gt"]
        value: str or number
            Value to use to filter.

        Attributes
        ----------
        columns : str or list
            columns on which the filter operation should be applied
        tablename : str
            Name of the table to which columns belong
        method : str
            value representing the comparision operator.
            Admissible value:  ["lt","le","eq","ne","ge,"gt"]
        value: str or number
            Value to use to filter.
        wherestr : str
            SQL where clause to be used for filtering

        Raises
        ------
        IdaDataBaseError
            * The value for filtering is not a string or a number
            * The filtering method is not suppoted
        Notes
        -----
        This object is considered as private, and should be called internally
        by IdaDataFrame instances.
        """
        # Sanity checks
        if isinstance(columns, six.string_types):
            columns = [columns]
        if isinstance(value, six.string_types):
            value = "'" + value + "'"
        if not ((isinstance(value,six.string_types)|isinstance(value,Number))):
            raise IdaDataBaseError("Value for Filterquery is expected to be a string or a number. Type %s"%str(type(value)))
        dictmethod = {"lt": " < ", "le": " <= ",  "eq": " = ", "ne": " != ", "ge": " >= ", "gt": " > "}
        if method not in dictmethod.keys():
            raise IdaDataBaseError("The filtering method is not suppoted. Admissible values for method argument are %s"%list(dictmethod.keys()))

        self.columns = columns     # TO DEPRECATE
        self.tablename = tablename
        self.method = method       # TO DEPRECATE
        self.value = value         # TO DEPRECATE
        self.wherestr = ("(\"" + ("\"" + str(dictmethod[method]) + str(value) + " AND \"").join(columns) +
                        "\"" +  str(dictmethod[method]) + str(value) + ")")

    # TO DEPRECATE
    @property
    def query(self):
        """
        Return an SQL query like "SELECT * FROM %s WHERE <WHERESTR>", where
        <WHERESTR> is the value of the attribute "wherestr".
        """
        return "SELECT * FROM %s WHERE " + self.wherestr

    def __and__(self, other):
        """
        Combine two FilterQuery instances with the operator "&" (AND)
        For example : (idadf['sepal_length'] < 5) & (idadf[petal_width'] > 3)
        """
        self._combine_check(other)
        newquery = deepcopy(self)
        newquery.wherestr = "(%s AND %s)"%(self.wherestr,other.wherestr)
        return newquery

    def __or__(self, other):
        """
        Combine two FilterQuery instances with the operator "|" (OR)
        For example : (idadf['sepal_length'] < 5) | (idadf[petal_width'] > 3)
        """
        self._combine_check(other)
        newquery = deepcopy(self)
        newquery.wherestr =  "(%s OR %s)"%(self.wherestr,other.wherestr)
        return newquery

    def __xor__(self, other):
        """
        Combine two FilterQuery instances with the operator "^" (XOR)
        For example : (idadf['sepal_length'] < 5) ^ (idadf[petal_width'] > 3)
        """
        self._combine_check(other)
        newquery = deepcopy(self)
        newquery.wherestr = ("((NOT %s AND %s) OR (%s AND NOT %s))"%(self.wherestr,other.wherestr,self.wherestr,other.wherestr))
        return newquery

    def _combine_check(self, other):
        """
        Check if the name of two  FilterQuery is the same. Raise an
        IdaDataBaseError in case it is different.
        This function is used before performing logical operations between
        FilterQuery instances.

        Raises
        ------
        IdaDataBaseError
        """
        if self.tablename != other.tablename:
            raise IdaDataBaseError("Combining filtering criterions from "+
                                   "columns belongings to different tables "+
                                   "is not possible.")