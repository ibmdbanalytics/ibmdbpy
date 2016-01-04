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

"""
The module contains the function that is used to modify or
create columns in an IdaDataFrame based on aggreation
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from builtins import str
from future import standard_library
standard_library.install_aliases()

from numbers import Number
from collections import OrderedDict

import ibmdbpy
from ibmdbpy.exceptions import IdaDataBaseError

def aggregate_idadf(idadf, method, other, swap = False):
    """
    Modify internal state variables to represent the aggregation of columns
    of an IdaDataFrame or IdaSeries in ibmdbpy.

    The following comparison operators are supported: +, \*, /, -, //, %, \*\*.

    The syntax is similar to Pandas.

    Parameters
    ----------
    idadf : IdaDataFrame or IdaSeries
        IdaDataFrame or IdaSerie on the left (if swap is False)
    method : str
        Aggregation method that is computed: the following values are 
        admissible: "add","mul","div","sub","floordiv","mod","neg","pow"
    other: Number or IdaDataFrame or IdaSeries
        Another object that idadf will be aggregated with (on the right if swap is False).
    swap : bool, default: False
        Internally used to handle cases where the call is made reflexively,
        that is when the main IdaDataFrame/IdaSeries is not on the left.
        If swap is True, this also implies that other is not of type IdaDataFrame/IdaSeries.

    Returns
    -------
    Aggregated IdaDataFrame or IdaSeries

    Raises
    ------
    ValueError
        Aggregation method not supported.
    TypeError
         Type not supported for aggregation.

    Examples
    --------
    >>> idairis['SepalLength'] = idairis['SepalLength'] * 2
    ...

    Notes
    -----
    It is not possible to create aggregations between columns that are stored 
    in different dashDB/DB2 tables.

    """
    def swap_manager(left, right, swap = False):
        if swap:
            left, right = right, left
        return (left, right)

    #Swap values in case of reflexive call
    # TODO : Override in IdaSeries instead of including the logic here.
    if swap:
        idadf, other = other, idadf

    simplemethod = {"add": " + ", "mul": " * ",  "div": " / ", "sub": " - "}
    complexmethod = {"floordiv" : " FLOOR(%s/%s) ",
                     "mod" : " MOD(%s,%s) ",
                     "neg" : " -%s%s ",
                     "pow" : " POWER(%s,%s)"} # overflow risk, to handle

    all_methods = list(simplemethod.keys())+list(complexmethod.keys())
    if method not in all_methods:
        raise ValueError("Admissible values for method argument are %s." %str(all_methods)[1:-1])

    columndict = OrderedDict()

    if isinstance(idadf, ibmdbpy.IdaDataFrame):

        for index, column in enumerate(idadf.internal_state.columndict.keys()):
            column_value = idadf.internal_state.columndict[column]
            if other is None: # this is for now just the neg case
                left, right = swap_manager(column_value, '')
            elif isinstance(other, Number):
                left, right = swap_manager(column_value, other, swap)
            elif isinstance(other, ibmdbpy.IdaSeries):
                left, right = swap_manager(column_value, "%s"%list(other.internal_state.columndict.values())[0], swap)
            elif isinstance(other, ibmdbpy.IdaDataFrame):
                if len(idadf.columns) != len(other.columns):
                    if len(other.columns) != 1:
                        raise IdaDataBaseError("Number of columns of other "+
                                               "IdaDataFrame should be either "+
                                               "equal to aggregated IdaDataFrame"+
                                               "or equal to 1.")
                    left, right = swap_manager("%s"%column_value, "%s"%list(other.internal_state.columndict.values())[0], swap)
                else:
                    left, right = swap_manager("%s"%column_value, "%s"%list(other.internal_state.columndict.values())[index], swap)
            else:
                raise TypeError("Aggregation method not supported. Unsupported type for aggregation: %s"%type(other))

            if method in simplemethod:
                columndict[column] = "(%s%s%s)"%(left, simplemethod[method], right)
            elif method in complexmethod:
                agg = complexmethod[method] %(left, right)
                columndict[column] = "(%s)"%agg

        newidadf = idadf._clone()
        for key,value in columndict.items():
            newidadf.internal_state.columndict[key] = value

        newidadf.internal_state.update()
        # REMARK: Don't need to reset some attributes ?
        return newidadf

    if isinstance(idadf, ibmdbpy.IdaSeries):
        columnname = idadf.internal_state.columndict.keys()[0]
        if other is None: # this is for now just the neg case
            left, right = swap_manager("\"%s\""%columnname, '')
        elif isinstance(other, Number):
            left, right = swap_manager("\"%s\""%columnname, other, swap)
        elif isinstance(other, ibmdbpy.IdaSeries):
            left, right = swap_manager("\"%s\""%columnname, "\"%s\""%other.columns[0], swap)
        else:
            raise TypeError("Type not supported for aggregation: " + str(type(other)))

        if method in simplemethod:
            columndict[columnname] = "(%s%s%s)"%(left, simplemethod[method], right)
        elif method in complexmethod:
            agg = complexmethod[method] %(left, right)
            columndict[columnname] = "(%s)"%agg

        newidaseries = idadf._clone()
        newidaseries.internal_state.columndict[key] = columndict[columnname]
        newidaseries.internal_state.update()
        return newidaseries