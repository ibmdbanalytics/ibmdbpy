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
Classes used for subsetting IdaDataFrames.
Indexer currently available : loc
"""
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()

import warnings

import six
from ibmdbpy.exceptions import IdaDataBaseError

#----------------------------------------------------------------------------
# Class for the loc object for IdaDataFrames

class Loc(object):
        """
        The Loc class is used to select and project IdaDataFrames.
        It implements a Pandas-like interface.
        """
        def __init__(self, idadf):
            self.idadf = idadf

        def __getitem__(self, key):
            """
            Use the loc object of an IdaDataFrame or IdaSeries instance to
            do projection or selection in a table.
            
            Notes
            -----
            The determinism of the result is guaranteed only if the IdaDataFrame
            has a valid indexer. 
            
            Examples
            --------
            >>> idairis.loc[0:49] # Select the first 50 rows
            >>> idairis.loc[2, "SepalLength"] # Select the 3rd rows and column "SepalLength"
            >>> idairis.loc[0:len(idairis), ["SepalLength", "SepalWidth"]] # Select all rows and columns "SepalLength", "SepalWidth"
            """
            if type(key) is tuple:
                if len(key) > 2:
                    raise ValueError("Too many indexer (expected 2)")
                else:
                    index = key[0]
                    cols = key[1]
            else:
                index = key
                cols = None


            if isinstance(index, six.integer_types):
                if self.idadf.indexer is None:
                    if (index < 0)|(index > self.idadf.shape[0]):
                        raise KeyError("The label [%s] is not in the [index]" %(index))
            if isinstance(index, list):
                if self.idadf.indexer is None:
                    if False in [isinstance(x, six.integer_types) for x in index]:
                        raise IdaDataBaseError("The IdaDataFrame has no indexer, so 'index' should be an integer or a list of integers")
                    for x in index:
                        if (x < 0)|(x > self.idadf.shape[0]):
                            raise ValueError("The index [%s] is out of range" %(index))

            if cols is not None:
                if isinstance(cols, six.string_types):
                    if cols not in self.idadf.columns:
                            raise KeyError("The label %s is not in the [columns]" %(cols))
                    newidadf = self.idadf._clone_as_serie(cols)
                else:
                    not_existing = [col for col in cols if col not in self.idadf.columns]
                    if not_existing:
                        raise KeyError("The label [%s] is not in the [columns]" %(not_existing))
                    newidadf = self.idadf._clone()

                for col in newidadf.internal_state.columndict.keys():
                    if col not in cols:
                        del newidadf.internal_state.columndict[col]

                if self.idadf.indexer is not None:
                    if self.idadf.indexer not in cols:
                        newidadf.internal_state.columndict[self.idadf.indexer] = "\""+self.idadf.indexer+"\""

                newidadf.internal_state.index = index
                newidadf.internal_state.update()
                #newidadf._reset_attributes(["shape", "dtypes", "index", "columns"]) # this was causing troubles 

                if self.idadf.indexer is not None:
                    if self.idadf.indexer not in cols:
                        
                        del newidadf.internal_state.columndict[self.idadf.indexer]
                        newidadf.internal_state.update()
                        newidadf._reset_attributes(["shape", "dtypes", "index", "columns"])


            else:
                newidadf = self.idadf._clone()
                newidadf.internal_state.index = index
                newidadf.internal_state.update()
                newidadf._reset_attributes(["shape", "index"])

            if self.idadf.indexer is None:
                if not " ORDER BY " in self.idadf.internal_state.get_state():
                    warnings.warn("Row order is not guaranteed if no indexer" +
                                  " was given and the dataset was not sorted")
            return newidadf