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

from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()

from .correlation import  pearson, spearman

from .entropy import entropy, entropy_stats

from .info_gain import info_gain

from .gain_ratio import gain_ratio

from .symmetric_uncertainty import su

from .gini import gini, gini_pairwise
from .discretize import discretize

from .chisquared import chisquared
from .tstats import ttest 

