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
Test module for IdaDataFrameObjects
"""
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()

class Test_Filtering(object):
    # Test combinations
    def test_Filtering_lt(self, idadf):
        mean = idadf.mean()
        if not mean.empty:
            ida = idadf[idadf[mean.index[0]] < mean[0]]
            assert(ida.max()[0] < mean[0])
            
    def test_Filtering_le(self, idadf):
        mean = idadf.mean()
        if not mean.empty:
            ida = idadf[idadf[mean.index[0]] <= mean[0]]
            assert(ida.max()[0] <= mean[0])
        pass

    def test_Filtering_eq(self, idadf):
        maxi = idadf.max()
        if not maxi.empty:
            ida = idadf[idadf[maxi.index[0]] == maxi[0]]
            assert(ida.max()[0] == maxi[0])
            assert(ida.min()[0] == maxi[0])
        pass
    
    def test_Filtering_neq(self, idadf):
        maxi = idadf.max()
        if not maxi.empty:
            ida = idadf[idadf[maxi.index[0]] != maxi[0]]
            assert(ida.max()[0] != maxi[0])
            assert(ida.min()[0] != maxi[0])
        pass

    def test_Filtering_ge(self, idadf):
        mean = idadf.mean()
        if not mean.empty:
            ida = idadf[idadf[mean.index[0]] >= mean[0]]
            assert(ida.max()[0] >= mean[0])
        pass

    def test_Filtering_gt(self, idadf):
        mean = idadf.mean()
        if not mean.empty:
            ida = idadf[idadf[mean.index[0]] > mean[0]]
            assert(ida.max()[0] > mean[0])
        pass


class Test_FilterQuery(object):
    # Test combinations
    def test_FilterQuery_and(self, idadf):
        maxi = idadf.max()
        mini = idadf.min()
        if not (maxi.empty | mini.empty):
            ida = idadf[(idadf[mini.index[0]] > mini[0])&(idadf[maxi.index[0]] < maxi[0])]
            assert(ida.max()[0] < maxi[0])
            assert(ida.min()[0] > mini[0])
        pass

    def test_FilterQuery_or(self, idadf):
        maxi = idadf.max()
        mini = idadf.min()
        if not (maxi.empty | mini.empty):
            ida = idadf[(idadf[mini.index[0]] == mini[0])|(idadf[maxi.index[0]] == maxi[0])]
            head = ida.head()
            for value in head.values:
                assert((value[0] == mini[0])|(value[0] == maxi[0]))         
        pass

    def test_FilterQuery_xor(self, idadf):
        mini = idadf.min()
        if not mini.empty:
            ida = idadf[(idadf[mini.index[0]] >= mini[0])^(idadf[mini.index[0]] == mini[0])]
            assert(ida.min()[0] > mini[0])
        pass

    def test_FilterQuery_error(self, idadf):
        pass