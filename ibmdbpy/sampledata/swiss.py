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

'''
This module provides a data set containing standardized fertility measure and
socio-economic indicators for each of 47 French-speaking provinces of
Switzerland at about 1888. As user, you can import the object 'swiss' which
is a pandas dataframe with 47 rows and the following fields:
    swiss['Fertility']
    swiss['Agriculture']
    swiss['Examination']
    swiss['Education']
    swiss['Catholic']
    swiss['Infant.Mortality']

Examples
--------
>>> from ibmdbpy.sampledata.swiss import swiss
'''

from __future__ import absolute_import
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from future import standard_library
standard_library.install_aliases()
from os.path import dirname, join
import pandas as pd

swiss = pd.read_csv(join(dirname(__file__), 'swiss.txt'), index_col = 0)