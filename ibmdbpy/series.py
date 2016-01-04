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
idaSeries
"""

# Ensure Python 2 compatibility
from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals
from __future__ import absolute_import
from builtins import super
from future import standard_library
standard_library.install_aliases()

from copy import deepcopy

from lazy import lazy

import ibmdbpy

class IdaSeries(ibmdbpy.IdaDataFrame):
    """
    IdaSeries can be considered as a different version of IdaDataFrame
    objects that have only one column and can be thus represented
    as pandas.Series to the user.
    """
    def __init__(self, idadb, tablename, indexer, column):
        super(IdaSeries, self).__init__(idadb, tablename, indexer)
        self.column = column

    ##### legacy
    @lazy
    def columns(self):
        return [self.column]

# TODO : Override all methods for which the behavior, i.e. the output is
# different in comparision with the one of an IdaDataFrame. For now the
# disjunction are implemented on the functions only.

    def min(self):
        result = super(IdaSeries, self).min()
        #import pdb; pdb.set_trace()
        return result[0]
        
    def max(self):
        result = super(IdaSeries, self).max()
        #import pdb; pdb.set_trace()
        return result[0]

    def _clone(self):
        """
        Clone an IdaSeries.
        """
        newida = IdaSeries(self._idadb, self._name, self.indexer, self.column)
        newida.internal_state.name = deepcopy(self.internal_state.name)
        newida.internal_state.ascending = deepcopy(self.internal_state.ascending)
        #newida.internal_state.views = deepcopy(self.internal_state.views)
        newida.internal_state._views = deepcopy(self.internal_state._views)
        newida.internal_state._cumulative = deepcopy(self.internal_state._cumulative)
        newida.internal_state.order = deepcopy(self.internal_state.order)
        return newida