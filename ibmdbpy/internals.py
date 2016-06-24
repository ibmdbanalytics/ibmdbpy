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
Definition of internal tools for non-destructive modification of IdaDataFrames.
"""

# Python 2 compatibility
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from builtins import str
from builtins import range
from future import standard_library
standard_library.install_aliases()

from functools import wraps, partial
from numbers import Number
from collections import OrderedDict

from lazy import lazy
import six

import ibmdbpy

from copy import deepcopy

def idadf_state(function=None, force=False):
    """
    View management for IdaDataFrame.
    This decorator creates the view reflecting the current state of an
    IdaDataFrame, so that the operation it decorates can work on this "virtual"
    version.
    """
    if function is None:
        return partial(idadf_state, force=force)
    @wraps(function)
    def wrapper(self, *args, **kwds):
        # Note: 5 is arbitrary. Maybe not the most relevant threshold
        # Means that not view should be created if the idadataframe was 
        # not modified at least 5 times. 
        if (len(self.internal_state.views) < 5)&(force is False): 
            # Here, avoid creating a view but putting in the stack the string
            # that corresponds to the state of the idadataframe
            if self.internal_state.views:
                self.internal_state.viewstack.append("(" + self.internal_state.get_state() + ")")
            try:
                result = function(self, *args, **kwds)
            except:
                raise
            finally:
                if self.internal_state.views:
                    if self.internal_state.viewstack:
                        self.internal_state.viewstack.pop()
        else:
            self.internal_state._create_view()
            try:
                result = function(self, *args, **kwds)
            except:
                raise
            finally:
                self.internal_state._delete_view()
        return result
    return wrapper

class InternalState(object):
    """
    Records and maintains an internal state of an IdaDataFrame.
    This class is used internally to manage modified version of IdaDataFrames
    without modifying the real version In-DataBase.
    """
    def __init__(self, idadf):
        """
        Constructor for InternalSate objects. InternalState objects shall be
        created only by IdaDataFrame/IdaSeries objects.

        Parameters
        ----------
        idadf : str
            IdaDataFrame to which self relates.

        Attributes
        ----------
        _idadf : IdaDataFrame
            IdaDataFrame to which self relates.
        name : str
            In database Name of the idaobject its belongs to.
        ascending : bool, default: True
            Indicate if the dataframe should be sorted in ascending or descending order.
        columns: str, default: '*'
            Columns that are selected in the IdaDataFrame, the order determine
            the order they are displayed too. Follow this model : ""Col1", "Col2", "Col3"".
        order: str, default: ''
            "Order by" part of a corresponding SQL query. Follow this model :
            " ORDER BY "Col1", "Col2", "Col3" ASC".
        _views : list of str
            List of nested SQL queries that describe the state of the
            IdaDataFrame. String that are stored  in this list may not be
            modified.
        _cumulative : str
            Cumulative represent the most current nested SQL queries. The main
            different with those in _views is that cumulative may still be
            modified. When the state of the IdaDataFrame is modified in the way
            that it is not possible to modify cumulative without violating the
            order the operation where may, the cumulative function is stored
            in _views, and a new cumulative gets created.
        viewstack : list of str
            Stack of view names that are created dynamically. Used to memorize
            the name of the temporary views so that they can get  deleted when
            the operation is finished.
        """
        self._idadf = idadf
        self.name = idadf._name
        self.tablename = idadf.tablename
        self.ascending = True # per default
        self.index = None
        self.order = None

        self._views = []
        self._cumulative = []
        self.viewstack = []

    @lazy
    def columndict(self):
        """
        Returns an ordered dictionnary that contains available columns as keys
        and their "real" expression as values, exactly in the way they should
        appear in modified SQL queries for creating views.
        """
        #return OrderedDict((cols,"\""+cols+"\"") for cols in self._idadf.columns)
        columns = deepcopy(OrderedDict((cols,"\""+cols+"\"") for cols in self._idadf.columns))
        
        #Remove the quotation marks enclosing DB2GSE functions
        #ibmdbpy stores and keeps track of columns internally enclosing them
        #with double quotation marks, but they must be removed for functions, 
        #so that the SQL query is interpreted correctly
        
        #TODO: Surround the geometries with ST_AsText()
        #so that the database retrieves geometries as CLOB with WKT instead 
        #of a geometry (which internally is a STRUCT type).
        #Although this STRUCT type has the method getSubString() of CLOB types, 
        #it makes jaydebeapi raise a warning of no mapping of STRUCT type
        #Pending to do this because knowing the type of a column by its name 
        #requires accessing the dtypes attribute, which is usually erased after
        #manipulating the Ida(Geo)DataFrame. In order to do this, refreshing
        #instead of erasing dtypes attribute would be a better approach

        for column in columns.keys():
            #if the column is an ST_ function
            if columns[column].find('ST_') == 0:
                columns[column] = columns[column][1:-1]
                
        return columns

    @property
    def views(self):
        """
        Returns the set of all views, including the cumulating view.
        """
        return self._views + self._cumulative

    @property
    def current_state(self):
        """
        If some temporary view have been created, i.e. the InternalState was
        modified and viewstack in not empty, returns the name of the last
        created view, otherwise returns the name of the table.
        """
        if self.viewstack:
            return self.viewstack[-1]
        else:
            return self.name

    def _create_view(self, viewname = None):
        """
        Creates a view that represent the current state of the idea dataframe

        Arguments
        ---------
        viewname : str, optional
            Name of the view to be created, if not given, then a unique name
            view will be generated.
        """
        if not self.views : return
        if viewname:
            view = viewname
        else:
            prefix = "TEMP_VIEW_%s_"%(self.tablename)
            view = self._idadf._idadb._get_valid_viewname(prefix)

        query = "CREATE VIEW \"%s\" AS (%s)"%(view, self.get_state())
        self._idadf._prepare_and_execute(query, autocommit = True)
        self.viewstack.append(view)
        return

    def _delete_view(self, viewname = None):
        """
        Deletes a view related to the internal state

        Arguments
        ---------
        viewname : str, optional
            Deletes the view that has this name. If no viewname is given,
            deletes the most recently created view, i.e. the one on the stack.
        """
        if viewname:
            view = viewname
        else:
            if self.viewstack:
                view = self.viewstack.pop()
            else:
                return
        query = "DROP VIEW \"" + view + "\""
        self._idadf._prepare_and_execute(query, autocommit = True)
        return

    def __del__(self):
        """
        Overriding the destructor for InternalState object.
        Making sure all the views in the stack are deleted before deleting self.
        """
        if self.viewstack:
            while self.viewstack:
                self._delete_view(self.viewstack.pop())

    def update(self, filter_query=None):
        """
        Update _views and/or _cumulative with an SQL select expression that
        corresponds to the lastest modification of the internal state.
        Determines automatically if it is enough to replace _cumulative
        or to store it in _views and create a new _cumulative view.
        The situation, in which we should stop cumulating are :
        Filter, order, selection (of rows)
        """
        if isinstance(filter_query, ibmdbpy.filtering.FilterQuery):
            query = filter_query.query
            #self.stop_cumulating_columns() # not sure ?
            self._views = self._views + self._cumulative
            self._cumulative = []
            self._views.append(query)
        elif self.order is not None:
            # Assumption : order is the only modification
            query = "SELECT * FROM (SELECT * FROM %s"+self.get_order()+")"
            self.order = None
            #self.stop_cumulating_columns()
            self._views = self._views + self._cumulative
            self._cumulative = []
            self._views.append(query)
        else:
            if self.index is None:
                query = ("SELECT " + self.get_columns() + " FROM %s")
                self._cumulative = [query]
            else:
                if isinstance(self.index, Number):
                    indexstring = " = " + str(self.index)
                elif isinstance(self.index, six.string_types):
                    if "'" in self.index:
                        self.index = self.index.replace("'", "\'")
                    indexstring = " = '" + str(self.index) + "'"
                elif isinstance(self.index, list):
                    indexstring = " in (" + str(self.index)[1:-1] + ")" #[1:-1] to strip the brackets
                elif isinstance(self.index, slice):
                    start = self.index.start
                    if not start:
                        start = 0
                    stop = self.index.stop
                    if not self.index.stop:
                        stop = self._idadf.shape[0]
                    if self.index.step:
                        # TODO: Improve this by using a modulo operation instead of listing
                        # This would prevent SQL overflow for big datasets
                        indexlist = list(range(start, stop)[start:stop:self.index.step])
                        indexstring = " in (" + str(indexlist)[1:-1] + ")"
                    else:
                        indexstring = " BETWEEN " + str(start) + " AND " + str(stop)

                columns = self.get_columns()
                #columns = self._idadf._get_columns()
                
                #columns = "\"" + "\",\"".join(set(self._idadf._get_columns())|set(self._idadf.columns)) + "\"" # TODO: Check if it is good
                #if columns == "*":
                    #columns = "\"" + "\",\"".join(self._idadf.columns) + "\""

                if self._idadf.indexer:
                    query = ("SELECT " + columns + " FROM %s "+ "WHERE " +
                             self._idadf.indexer + indexstring)
                else:
                    query = ("SELECT " + columns + " FROM (SELECT TEMP.*, "+
                            "(ROWNUMBER() OVER()-1) AS RN FROM %s AS TEMP) "+
                            "WHERE RN " + indexstring)
                self.index = None

                #self.stop_cumulating_columns()
                self._views = self._views + self._cumulative
                self._cumulative = []
                self._views.append(query)


    def get_state(self):
        """
        Returns the sql query corresponding to the current state of the
        IdaDataFrame. The query is created by nesting of all strings in views
        (which is the concatenation of _views and _cumulative).
        """
        if not self.views:
            return ("SELECT " + self.get_columns() + " FROM " + self.name)

        query = "%s"
        for index, view in enumerate(self.views[::-1]):
            if index == (len(self.views)-1):
                view = view % self.name
            if index != 0:
                view = "(" + view + ")"
            query = query %(view)
        return query

    def get_columns(self):
        """
        Returns a string containing selected columns in the current state.
        For each column in in columndict, column expression are either :
            * The value of the column if it was unchanged ("<CONAME>")
            * The value of the column following by " AS \"<COLNAME>\""
        """
        columns = []
        for key,value in self.columndict.items():
            if value[1:-1] == key:
                columns.append(value)
            else:
                columns.append("%s AS \"%s\""%(value, key))

        return ",".join(columns)


    def get_order(self):
        """
        Returns the "Order by" clause of a corresponding SQL query, according
        to the value of the attribute order. If order is None, returns an
        empty string.

        Examples
        --------
        >>> self.get_order()
        " ORDER BY "Col1" ASC, "Col2" ASC, "Col3" ASC"
        """
        if not self.order:
            return ''
        else:
            if self.ascending:
                return (" ORDER BY \"" + "\" ASC, \"".join(self.order) + "\" ASC")
            else:
                return (" ORDER BY \"" + "\" DESC, \"".join(self.order) + "\" DESC")

    def is_default(self):
        """
        Checks if the InternalState object corresponds to the default values.

        Returns
        -------
        bool
            1 : The InternalState has default values
            0 : The InternalState is modifed
        """
        return self.get_state() == InternalState(self.name).get_state()

    def set_order(self, order, ascending):
        """
        Changes order in the internal state of the IdaDataframe.
        """
        self.order = order
        self.ascending = ascending

    def reset_order(self):
        """
        Resets the state to original order. Delete all views that have an ORDER
        clause. Based on the assumption that ORDER is the only modification
        of those queries.
        """
        # This is based on the assumption than no other modifications than
        # sorting was done when sorting the idadf
        self._views = [view for view in self._views if "ORDER BY" not in view]

    def stop_cumulating_columns(self):
        # TODO: do not use anymore ??
        # You actually don't want to use column that you have not selected anymore ? 
        # except if you want to sum maybe ...
        """
        Management of views to ensure that some specific operations such as
        Filter, order, selection are registred in the list of views so that
        the real order of operation is respected.
        Here is what the function does:
        If _cumulative is not None,
        then the _cumulative is augmented with all available columns in the
        database, if any and then appended in _views.
        _cumulative is then reset to new query that only select the columns
        that were selected on that point. This is made so, because it allows
        users to still use all available columns in the table, even if they are
        not selected in the current state.
        """
        # TODO: Improve documentation for this method (experimental)

        # Add all columns available originally in the table in the history
        if self._cumulative:
            all_columns = self._idadf._get_all_columns_in_table()
            deleted_columns = []
            for column in all_columns:
                if column not in list(self.columndict.keys()):
                    deleted_columns.append(column)

            if deleted_columns:
                for column in deleted_columns:
                     self.columndict[column] = ("\""+column+"\"")
                self._cumulative = ["SELECT " + self.get_columns() + " FROM %s"]

            self._views = self._views + self._cumulative
            self._cumulative = []

            # Select only the columns that were available in the representation
            # Those 2 steps are made so to enable users to still use columns
            # that were not selected anymore in the current table representation
            # but still existing in the idadataframe

            if deleted_columns:
                for column in deleted_columns:
                    del self.columndict[column]
                self.columndict = OrderedDict((cols,"\""+cols+"\"") for cols in self.columndict)
                self._cumulative = ["SELECT " + self.get_columns() + " FROM %s"]
            else:
                self.columndict = OrderedDict((cols,"\""+cols+"\"") for cols in self.columndict)
