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

# Python 2 Compatibility
from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals
from __future__ import absolute_import
from builtins import dict
from builtins import zip
from builtins import str
from builtins import int
from future import standard_library
standard_library.install_aliases()

from collections import OrderedDict
import itertools
import math
import warnings
from numbers import Number

import pandas as pd
import numpy as np
import six

import ibmdbpy
from ibmdbpy.utils import chunklist

"""
Statistics module for IdaDataFrames
"""

def _numeric_stats(idadf, stat, columns):
    """
    Compute various stats from one or several numerical columns of an IdaDataFrame.

    Parameters
    ----------
    idadf : IdaDataFrame
        Data source.
    stat : str
        Name of the statistic to be computed.
    columns : str or list of str
        Name of the columns that belong to the IdaDataFrame.

    Returns
    -------
    Tuple
        One value for each column.

    Notes
    -----
    Currently, the following functions are supported: count, mean, median, std, 
    var, min, max, sum. Should return a tuple. Only available for numerical 
    columns.
    """
    # Calculate count, mean, median, std, var, min, max
    if isinstance(columns, six.string_types):
        columns = [columns]

    if isinstance(stat, six.string_types):
        if stat == "count":
            select_string = 'COUNT(\"' + '\"), COUNT(\"'.join(columns) + '\")'
        elif stat == "mean":
            select_string = ('AVG(CAST(\"' +
                             '\" AS FLOAT)), AVG(CAST(\"'.join(columns) +
                             '\" AS FLOAT))')
        elif stat == "median":
            return _get_percentiles(idadf, 0.5, columns).values[0]
        elif stat == "std":
            tuple_count = _numeric_stats(idadf, 'count', columns)
            count_dict = dict((x, y) for x, y in zip(columns, tuple_count))
            agg_list = []
            for column in columns:
                agg_list.append("STDDEV(\"%s\")*(SQRT(%s)/SQRT(%s))"
                                %(column, count_dict[column], count_dict[column]-1))
            select_string = ', '.join(agg_list)
        elif stat == "var":
            tuple_count = _numeric_stats(idadf, 'count', columns)
            count_dict = dict((x, int(y)) for x, y in zip(columns, tuple_count))
            agg_list = []
            for column in columns:
                agg_list.append("VAR(\"%s\")*(%s.0/%s.0)"
                                %(column, count_dict[column], count_dict[column]-1))
            select_string = ', '.join(agg_list)
        elif stat == "min":
            select_string = 'MIN(\"' + '\"), MIN(\"'.join(columns) + '\")'
        elif stat == "max":
            select_string = 'MAX(\"' + '\"), MAX(\"'.join(columns) + '\")'
        elif stat == "sum":
            select_string = 'SUM(\"' + '\"), SUM(\"'.join(columns) + '\")'

        name = idadf.internal_state.current_state

        return idadf.ida_query("SELECT %s FROM %s" %(select_string, name)).values[0]


def _get_percentiles(idadf, percentiles, columns):
    """
    Return percentiles over all entries of a column or list of columns in the 
    IdaDataFrame.

    Parameters
    ----------
    idadf : IdaDataFrame
    percentiles: Float or list of floats.
        All values in percentiles must be > 0  and < 1
    columns: String or list of string
        Name of columns belonging to the IdaDataFrame.

    Returns
    -------
        DataFrame
    """

    if isinstance(columns, six.string_types):
        columns = [columns]
    if isinstance(percentiles, Number):
        percentiles = [percentiles]

    name = idadf.internal_state.current_state

    # Get na values for each columns
    tuple_na = _get_number_of_nas(idadf, columns)
    nrow = idadf.shape[0]
    data = pd.DataFrame()
    for index_col, column in enumerate(columns):
        nb_not_missing = nrow - tuple_na[index_col]
        indexes = [float(x)*float(nb_not_missing-1) + 1 for x in percentiles]
        low = [math.floor(x) for x in indexes]
        high = [math.ceil(x) for x in indexes]
        tuplelist = []
        i = 0
        for flag in [((x+1) == y) for x, y in zip(low, high)]:
            if flag:
                tuplelist.append((i, i+1))
                i += 2
            else:
                tuplelist.append((i, i))
                i += 1
        unique = low + high
        unique = set(unique)
        unique = sorted(unique)
        unique = [str(x) for x in unique]
        indexes_string = ",".join(unique)
        df = idadf.ida_query("(SELECT \""+column+"\" AS \""+column+"\" FROM (SELECT "+
                        "ROW_NUMBER() OVER(ORDER BY \""+column+"\") as rn, \""+
                        column + "\" FROM (SELECT * FROM " + name +
                        ")) WHERE rn  in("+ indexes_string +"))")

        #indexvalues = list(df[df.columns[0]])
        indexvalues = list(df)
        #import pdb ; pdb.set_trace()
        #print(tuplelist)
        #print(indexvalues)
        indexfinal = [(float(str(indexvalues[x[0]]))+float(str(indexvalues[x[1]])))/2 for x in tuplelist]
        new_data = pd.DataFrame(indexfinal)
        data[column] = (new_data.T).values[0]

    percentile_names = [x for x in percentiles]
    data.index = percentile_names
    return data


def _categorical_stats(idadf, stat, columns):
    # TODO:
    """
    Computes various stats from one or several categorical columns of the IdaDataFrame.
    This is not implemented.

    Parameters
    ----------
    idadf : IdaDataFrame
    stat : str
        Name of the statistic function to be computed.
    columns : str or list of str
        Name of columns belonging to the IdaDataFrame.

    Returns
    -------
        Tuple.
    """
    # Calculates count, unique, top, freq
    raise NotImplementedError("TODO")

def _get_number_of_nas(idadf, columns):
    """
    Return the count of missing values for a list of columns in the IdaDataFrame.

    Parameters
    ----------
    idadf : IdaDataFrame
    columns : str or list
        One column as a string or a list of columns in the idaDataFrame.

    Returns
    -------
        Tuple
    """
    if isinstance(columns, six.string_types):
        columns = [columns]

    name = idadf.internal_state.current_state

    query_list = list()
    for column in columns:
        string = ("(SELECT COUNT(*) AS \"" + column + "\" FROM " +
                name + " WHERE \"" + column + "\" IS NULL)")
        query_list.append(string)

    query_string = ', '.join(query_list)

    # TODO: Improvement idea : Get nrow (shape) and substract by count("COLUMN")
    return idadf.ida_query("SELECT * FROM " + query_string, first_row_only = True)

def _count_level(idadf, columnlist=None):
    """
    Count distinct levels across a list of columns of an IdaDataFrame grouped 
    by themselves.


    Parameters
    ----------
    columnlist : list
        List of column names that exist in the IdaDataFrame. By default, these 
        are all columns in IdaDataFrame.

    Returns
    -------
        Tuple
        
    Notes
    -----
    The function assumes the follwing:
        * The columns given as parameter exists in the IdaDataframe.
        * The parameter columnlist is an optional list.
        * Columns are referenced by their own name (character string).
    """
    if columnlist is None:
        columnlist = idadf.columns

    name = idadf.internal_state.current_state

    query_list = []
    for column in columnlist:
        # Here cast ? 
        query_list.append("(SELECT COUNT(*) AS \"" + column +"\" FROM (" +
                          "SELECT \"" + column + "\" FROM " + name +
                          " GROUP BY \"" + column + "\" ))")
        #query_list.append("(SELECT CAST(COUNT(*) AS BIGINT) AS \"" + column +"\" FROM (" +
        #                  "SELECT \"" + column + "\" FROM " + name + " ))")

    query_string = ', '.join(query_list)
    column_string = '\"' + '\", \"'.join(columnlist) + '\"'
    return idadf.ida_query("SELECT " + column_string + " FROM " + query_string, first_row_only = True)

def _count_level_groupby(idadf, columnlist=None):
    """
    Count distinct levels across a list of columns in the IdaDataFrame grouped 
    by themselves. This is used to get the dimension of the resulting cross table.

    Parameters
    ----------
    columnlist : list
        List of column names existing in the IdaDataFrame. By default, these 
        are columns of self

    Returns
    -------
        Tuple
        
    Notes
    -----
    The function assumes the follwing:
        * The columns given as parameter exists in the IdaDataframe.
        * The parameter columnlist is a optional and is a list.
        * Columns are referenced by their own name (character string).
    """
    if columnlist is None:
        columnlist = idadf.columns

    name = idadf.internal_state.current_state

    column_string = '\"' + '\", \"'.join(columnlist) + '\"'
    query = (("SELECT COUNT(*) FROM (SELECT %s, COUNT(*) as COUNT "+
            "FROM %s GROUP BY %s ORDER BY %s, COUNT ASC)")
            %(column_string, name, column_string, column_string))
    return idadf.ida_query(query, first_row_only = True)

# TODO: REFACTORING: factors function should maybe return a tuple ?
def _factors_count(idadf, columnlist, valuelist=None):
    """
    Count non-missing values for all columns in a list (valuelist) over the 
    IdaDataFrame grouped by a list of columns(columnlist).

    Parameters
    ----------
    columnlist : list
        List of column names that exist in self.
    valuelist : list
        List of column names that exist in self.

    Assumptions
    -----------
        * The columns given as parameter exists in the IdaDataframe
        * The parameter columnlist is a optional and is a list
        * Columns are referenced by their own name (character string)

    Returns
    -------
        DataFrame
    """
    column_string = '\"' + '\", \"'.join(columnlist) + '\"'

    name = idadf.internal_state.current_state

    if valuelist is None:
        query = (("SELECT %s, COUNT(*) as COUNT FROM %s GROUP BY %s ORDER BY %s, COUNT ASC")
                %(column_string, name, column_string, column_string))
    else:
        agg_list = []
        for value in valuelist:
            query = "COUNT(\"%s\") as \"%s\""%(value,value)
            agg_list.append(query)

        agg_string =  ', '.join(agg_list)
        value_string = '\"' + '", "'.join(valuelist) + '\"'

        query = (("SELECT %s,%s FROM %s GROUP BY %s ORDER BY %s,%s ASC")
                %(column_string, agg_string, name, column_string, column_string, value_string))

    return idadf.ida_query(query)

def _factors_sum(idadf, columnlist, valuelist):
    """
    Compute the arithmetic sum over for all columns in a list (valuelist)
    over the IdaDataFrame grouped by a list of columns (columnlist).

    Parameters
    ----------
    columnlist : list
        List of column names that exist in self. 
    valuelist : list
        List of column names that exist in self. 

    Assumptions
    -----------
        * The columns given as parameter exists in the IdaDataframe
        * The parameter columnlist is a optional and is a list
        * Columns are referenced by their own name (character string)

    Returns
    -------
        DataFrame
    """
    column_string = '\"' + '\", \"'.join(columnlist) + '\"'

    name = idadf.internal_state.current_state

    agg_list = []
    for value in valuelist:
        query = "SUM(\"%s\") as \"%s\""%(value, value)
        agg_list.append(query)

    agg_string =  ', '.join(agg_list)
    value_string = '\"' + '", "'.join(valuelist) + '\"'

    query = (("SELECT %s,%s FROM %s GROUP BY %s ORDER BY %s,%s ASC")
            %(column_string, agg_string, name, column_string, column_string, value_string))

    return idadf.ida_query(query)

def _factors_avg(idadf, columnlist, valuelist):
    """
    Compute the arithmetic average for all columns in a list (valuelist) over 
    the IdaDataFrame grouped by a list of columns (columnlist).

    Parameters
    ----------
    columnlist : list
        List of column names that exist in self. 
    valuelist : list
        List of column names that exist in self. 

    Assumptions
    -----------
        * The columns given as parameter exists in the IdaDataframe
        * The parameter columnlist and valuelist are array-like
        * Columns are referenced by their own name (character string)

    Returns
    -------
        DataFrame
    """
    column_string = '\"' + '\", \"'.join(columnlist) + '\"'

    name = idadf.internal_state.current_state

    agg_list = []
    for value in valuelist:
        agg = (("CAST(AVG(CAST(\"%s\" AS DECIMAL(10,6))) AS DECIMAL(10,6)) \"%s\"")
              %(value, value))
        agg_list.append(agg)


    agg_string =  ', '.join(agg_list)
    value_string = '\"' + '", "'.join(valuelist) + '\"'

    query = (("SELECT %s,%s FROM %s GROUP BY %s ORDER BY %s,%s ASC")
            %(column_string, agg_string, name, column_string, column_string, value_string))

    return idadf.ida_query(query)

###############################################################################
### Pivot Table
###############################################################################

def pivot_table(idadf, values=None, columns=None, max_entries=1000, sort=None,
                factor_threshold=None, interactive=False, aggfunc='count'):
    """
    See IdaDataFrame.pivot_table
    """

    # TODO : Support index

    if aggfunc.lower() not in ['count', 'sum', 'avg', 'average', 'mean']:
        print("For now only 'count' and 'sum' and 'mean' as aggregation function is supported")
        return

    if (columns is None) & (factor_threshold is None):
        print("Please provide parameter factor_threshold for automatic selection of columns")
        return

    if isinstance(columns, six.string_types):
        columns = [columns]

    if isinstance(values, six.string_types):
        values = [values]

    if (values is None) and (aggfunc.lower() != "count"):
        raise ValueError("Cannot aggregate using another function than count if" +
                         "no value(s) was/were given")

    ####### Identify automatically categorical fields #########
    # Load distinct count for each and evaluate categorical or not
    data = idadf._table_def(factor_threshold) #
    if columns is None:
        factors = data.loc[data['VALTYPE'] == "CATEGORICAL", ['TYPENAME', 'FACTORS']]
        if len(factors) == 0:
            print("No categorical columns to tabulate")
            return
    else:
        factors = data.loc[columns, ['TYPENAME', 'FACTORS']]

    if sort == "alpha":
        factors.sort_index(inplace=True, ascending=1)
    elif sort == "factor":
        factors.sort(['FACTORS'], inplace=True, ascending=1)

    if columns is None:
        print("Automatic selection of columns :", factors.index.values)
        columns = factors.index.values

    nb_row = _count_level_groupby(idadf, factors.index.values)[0] * len(columns)
    nb_col = len(factors.index.values)

    nb_entries = nb_row * nb_col

    if nb_entries > max_entries: # Overflow risk
        print("Number of entries :", nb_entries)
        print("Value counts for factors:")
        factor_values = factors[['FACTORS']]
        factor_values.columns = ['']
        print(factor_values.T)
        print("WARNING :Attempt to make a table with more than " +
              str(max_entries)+ " elements. Either increase max_entries " +
              "parameter or remove columns with too many levels.")
        return

    print("Output dataframe has dimensions", nb_row, "x", (nb_col+1))
    if interactive is True:
        display_yes = ibmdbpy.utils.query_yes_no("Do you want to download it in memory ?")
        if not display_yes:
            return

    categorical_columns = list(factors.index)
    if aggfunc.lower() == 'count':
        dataframe = _factors_count(idadf, categorical_columns, values) # Download dataframe
    if aggfunc.lower() == 'sum':
        dataframe = _factors_sum(idadf, categorical_columns, values) # Download dataframe
    if aggfunc.lower() in ['avg', 'average', 'mean']:
        dataframe = _factors_avg(idadf, categorical_columns, values) # Download dataframe

    if values is not None:
        agg_values = values
    else: agg_values = aggfunc.upper()

    if isinstance(agg_values, six.string_types):
        agg_values = [agg_values]
    dataframe.columns = categorical_columns + agg_values # Name the aggregate column

    # Formatting result
    if len(agg_values) == 1:
        dataframe[None] = agg_values[0]
    else:
        catdataframe = dataframe[categorical_columns]
        dataframe = catdataframe.join(dataframe[agg_values].stack().reset_index(1))
        dataframe['level_1'] = pd.Categorical(dataframe['level_1'], agg_values)
        dataframe = dataframe.rename(columns={'level_1':None})
        dataframe = dataframe.sort([None] + categorical_columns)

    dataframe.set_index([None] + categorical_columns, inplace=True)
    dataframe = dataframe.astype(float)

    result = pd.Series(dataframe[dataframe.columns[0]])
    result.name = None

    return result

###############################################################################
### Descriptive statistics
###############################################################################

def describe(idadf, percentiles=[0.25, 0.50, 0.75]):
    """
    See IdaDataFrame.describe
    """
    if percentiles is not None:
        if isinstance(percentiles, Number):
            percentiles = [percentiles]
        if True in [(not isinstance(x, Number)) for x in percentiles]:
            raise TypeError("Argument 'percentiles' should be either a number or " +
                            "a list of numbers between 0 and 1")
        elif True in [((x >= 1) | (x <= 0)) for x in percentiles]:
            raise ValueError("Numbers in argument 'percentiles' should be between 0 and 1")

    # Improvement idea : We could use dtypes instead of calculating this everytime
    columns = idadf._get_numerical_columns()
    data = []
    if not columns:
        columns = idadf._get_categorical_columns()
        if not columns:
            raise NotImplementedError("No numerical and no categorical columns")
        else:
            raise NotImplementedError("Categorical only idaDataFrame are not handled currently")
            # TODO : Handle categorical columns
            data.append(_categorical_stats(idadf, "count", columns))
            data.append(_categorical_stats(idadf, "unique", columns))
            data.append(_categorical_stats(idadf, "top", columns))
            data.append(_categorical_stats(idadf, "freq", columns))
    else:
        data.append(_numeric_stats(idadf, "count", columns))
        data.append(_numeric_stats(idadf, "mean", columns))
        data.append(_numeric_stats(idadf, "std", columns))
        data.append(_numeric_stats(idadf, "min", columns))
        if percentiles is not None:
            perc = (_get_percentiles(idadf, percentiles, columns))
            for tup in perc.itertuples(index=False):
                data.append(tup)
        data.append(_numeric_stats(idadf, "max", columns))

    data = pd.DataFrame(data)
    data.columns = columns
    if percentiles is not None:
        percentile_names = [(str(int(x * 100)) + "%") for x in percentiles]
    else:
        percentile_names = [] 
    data.index = ['count', 'mean', 'std', 'min'] + percentile_names + ['max']
    
    # quick fix -> JDBC problems 
    #for column in data.columns:
    #    data[[column]] = data[[column]].astype(float)

    if isinstance(idadf, ibmdbpy.IdaSeries):
        data = pd.Series(data[data.columns[0]])

    return data


def quantile(idadf, q=0.5):
    """
    See IdaDataFrame.quantile
    """

    if isinstance(q, Number):
        q = [q]

    # Sanity check
    if True in [(not isinstance(x, Number)) for x in q]:
        raise TypeError("Argument 'q' should be either a number or " +
                        "a list of numbers between 0 and 1")
    elif True in [((x >= 1) | (x <= 0)) for x in q]:
        raise ValueError("Numbers in argument 'percentiles' should be between 0 and 1")

    columns = idadf._get_numerical_columns()
    if not columns:
        print(idadf.name + " has no numeric columns")
        return

    result = _get_percentiles(idadf, q, columns)

    if isinstance(q, list):
        if len(q) > 1:
            return result

    result = result.T
    result = result[result.columns[0]]
    result.name = q[0]
    result = result.astype('float')

    if len(result) == 1:
        result = result[0]

    return result

# Note : Not casting to double can lead to SQL overflow
# TODO: Has to be modified in ibmdbR

def cov(idadf, other = None):
    """
    See IdaDataFrame.cov
    """
    if isinstance(idadf, ibmdbpy.IdaSeries):
        raise TypeError("cov() missing 1 required positional argument: 'other'")

    columns = idadf._get_numerical_columns()
    if not columns:
        print(idadf.name + " has no numeric columns")
        return

    tuple_count = _numeric_stats(idadf, 'count', columns)
    count_dict = dict((x, int(y)) for x, y in zip(columns, tuple_count))

    agg_list = []

    combinations = [x for x in itertools.combinations_with_replacement(columns, 2)]
    columns_set = [{x[0], x[1]} for x in combinations]

    for column_pair in combinations:
        agg_list.append("COVARIANCE(\"" + column_pair[0] + "\",\"" +
                        column_pair[1] + "\")*(" +
                        str(min([count_dict[column_pair[0]],
                                 count_dict[column_pair[1]]])) + ".0/" +
                        str(min([count_dict[column_pair[0]],
                                 count_dict[column_pair[1]]])-1) + ".0)")

    agg_string = ', '.join(agg_list)

    name = idadf.internal_state.current_state

    data = idadf.ida_query("SELECT %s FROM %s"%(agg_string, name), first_row_only = True)

    tuple_list = []

    for column1 in columns:
        list_value = []
        for column2 in columns:
            for index, column_set in enumerate(columns_set):
                if {column1, column2} == column_set:
                    list_value.append(data[index])
                    break
        tuple_list.append(tuple(list_value))

    result = pd.DataFrame(tuple_list)
    result.index = columns
    result.columns = columns

    if len(result) == 1:
        result = result[0]

    return result

def corr(idadf, features=None,ignore_indexer=True):
    """
    See IdaDataFrame.corr
    """
    if isinstance(idadf, ibmdbpy.IdaSeries):
        raise TypeError("corr() missing 1 required positional argument: 'other'")
    # TODO: catch case n <= 1
    numerical_columns = idadf._get_numerical_columns()
    
    if not numerical_columns:
        print(idadf.name + " has no numeric columns")
        return
        
    if ignore_indexer is True:
        if idadf.indexer:
            if idadf.indexer in numerical_columns:
                numerical_columns.remove(idadf.indexer)
    
    #print(features)
    #target, features = ibmdbpy.utils._check_input(target, features)
    if features is not None:
        for feature in features:
            if feature not in numerical_columns:
                raise TypeError("Correlation-based measure not available for non-numerical columns %s"%feature)
    else:
        features = numerical_columns
    
    #if target not in columns:
    #    raise ValueError("%s is not a column of numerical type in %s"%(target, idadf.name))
    
    values = OrderedDict()
    
    combinations = [x for x in itertools.combinations(features, 2)]
    #columns_set = [{x[0], x[1]} for x in combinations]
    
    if len(features) < 64: # the limit of variables for an SQL statement is 4096, i.e 64^2
        agg_list = []
        for column_pair in combinations:
            agg = "CORRELATION(\"%s\",\"%s\")"%(column_pair[0], column_pair[1])
            agg_list.append(agg)
    
        agg_string = ', '.join(agg_list)
    
        name = idadf.internal_state.current_state
    
        data = idadf.ida_query("SELECT %s FROM %s"%(agg_string, name), first_row_only = True)
    
        for i, element in enumerate(combinations):
            if element[0] not in values:
                values[element[0]] = {}
            if element[1] not in values:
                values[element[1]] = {}
            values[element[0]][element[1]] = data[i]
            values[element[1]][element[0]] = data[i]
            
        result = pd.DataFrame(values).fillna(1)
    else:        
        chunkgen = chunklist(combinations, 100)
        
        for chunk in chunkgen: 
            agg_list = []
            for column_pair in chunk:
                agg = "CORRELATION(\"%s\",\"%s\")"%(column_pair[0], column_pair[1])
                agg_list.append(agg)
        
            agg_string = ', '.join(agg_list)
        
            name = idadf.internal_state.current_state
        
            data = idadf.ida_query("SELECT %s FROM %s"%(agg_string, name), first_row_only = True)
        
            for i, element in enumerate(chunk):
                if element[0] not in values:
                    values[element[0]] = OrderedDict()
                if element[1] not in values:
                    values[element[1]] = OrderedDict()
                values[element[0]][element[1]] = data[i]
                values[element[1]][element[0]] = data[i]
            
        result = pd.DataFrame(values).fillna(1)
    
    result = result.reindex(result.columns)
    if len(result) == 1:
        result = result[0]

    return result

### corrwith


def mad(idadf):
    """
    See IdaDataFrame.mad
    """
    columns = idadf._get_numerical_columns()
    if not columns:
        print(idadf.name + " has no numeric columns")
        return

    mean_tuple = _numeric_stats(idadf, "mean", columns)
    absmean_dict = dict((x, abs(y)) for x, y in zip(columns, mean_tuple))
    tuple_na = _get_number_of_nas(idadf, columns)

    agg_list = []
    for index_col, column in enumerate(columns):
        agg_list.append("SUM(ABS(\"" + column + "\" -" +
                        str(absmean_dict[column]) + "))/" +
                        str(idadf.shape[0] - tuple_na[index_col]))

    agg_string = ', '.join(agg_list)

    name = idadf.internal_state.current_state

    mad_tuple = idadf.ida_query("SELECT %s FROM %s"%(agg_string, name))
    result = pd.Series(mad_tuple.values[0])
    result.index = columns
    result = result.astype('float')

    if isinstance(idadf, ibmdbpy.IdaSeries):
        result = result[0]

    return result

def ida_min(idadf):
    """
    See idadataFrame.min
    """
    na_tuple = _get_number_of_nas(idadf, idadf.columns)
    min_tuple = _numeric_stats(idadf, "min", idadf.columns)
    if not hasattr(min_tuple,"__iter__") : min_tuple = (min_tuple,) # dirty fix 
    min_list = [np.nan if ((y > 0) and not isinstance(x, Number))
                else x for x, y in zip(min_tuple, na_tuple)]
    min_tuple = tuple(min_list)
    result = pd.Series(min_tuple)
    result.index = idadf.columns

    #if isinstance(idadf, ibmdbpy.IdaSeries):
     #   result = result[0]

    return result

def ida_max(idadf):
    """
    See idadataFrame.max
    """
    na_tuple = _get_number_of_nas(idadf, idadf.columns)
    max_tuple = _numeric_stats(idadf, "max", idadf.columns)
    if not hasattr(max_tuple,"__iter__") : max_tuple = (max_tuple,) # dirty fix 
    max_list = [np.nan if ((y > 0) and not isinstance(x, Number))
                else x for x, y in zip(max_tuple, na_tuple)]
    max_tuple = tuple(max_list)
    result = pd.Series(max_tuple)
    result.index = idadf.columns

    #if isinstance(idadf, ibmdbpy.IdaSeries):
      #  result = result[0]

    return result

def count(idadf):
    """
    See IdaDataFrame.count
    """
    count_tuple = _numeric_stats(idadf, "count", idadf.columns)
    result = pd.Series(count_tuple)
    result.index = idadf.columns
    result = result.astype(int)

    if isinstance(idadf, ibmdbpy.IdaSeries):
        result = result[0]

    return result

def count_distinct(idadf):
    """
    See IdaDataFrame.count_distinct
    """
    result = pd.Series(_count_level(idadf))
    result.index = idadf.columns
    result = result.astype(int)

    if isinstance(idadf, ibmdbpy.IdaSeries):
        result = result[0]

    return result

def std(idadf):
    """
    See IdaDataFrame.std
    """
    columns = idadf._get_numerical_columns()
    if not columns:
        warnings.warn("%s has no numeric columns"%idadf.name)
        return pd.Series()

    std_tuple = _numeric_stats(idadf, "std", columns)

    result = pd.Series(std_tuple)
    result.index = columns

    if isinstance(idadf, ibmdbpy.IdaSeries):
        result = result[0]

    return result

def var(idadf):
    """
    See IdaDataFrame.var
    """
    columns = idadf._get_numerical_columns()
    if not columns:
        warnings.warn("%s has no numeric columns"%idadf.name)
        return pd.Series()

    var_tuple = _numeric_stats(idadf, "var", columns)

    result = pd.Series(var_tuple)
    result.index = columns

    if isinstance(idadf, ibmdbpy.IdaSeries):
        result = result[0]

    return result

def mean(idadf):
    """
    See IdaDataFrame.mean
    """
    columns = idadf._get_numerical_columns()
    if not columns:
        warnings.warn("%s has no numeric columns"%idadf.name)
        return pd.Series()

    mean_tuple = _numeric_stats(idadf, "mean", columns)

    result = pd.Series(mean_tuple)
    result.index = columns

    if isinstance(idadf, ibmdbpy.IdaSeries):
        result = result[0]

    return result

def ida_sum(idadf):
    """
    See IdaDataFrame.sum
    """
    #Behave like having the option "numeric only" to true
    columns = idadf._get_numerical_columns()
    if not columns:
        warnings.warn("%s has no numeric columns"%idadf.name)
        return pd.Series()

    sum_tuple = _numeric_stats(idadf, "sum", columns)

    result = pd.Series(sum_tuple)
    result.index = columns

    if isinstance(idadf, ibmdbpy.IdaSeries):
        result = result[0]

    return result

def median(idadf):
    """
    See IdaDataFrame.median
    """
    #Behave like having the option "numeric only" to true
    columns = idadf._get_numerical_columns()
    if not columns:
        warnings.warn("%s has no numeric columns"%idadf.name)
        return pd.Series()

    median_tuple = _numeric_stats(idadf, "median", columns)

    result = pd.Series(median_tuple)
    result.index = columns

    if isinstance(idadf, ibmdbpy.IdaSeries):
        result = result[0]

    return result
