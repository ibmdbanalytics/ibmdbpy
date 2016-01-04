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
This module provides Fisher's Iris flower data set. It provides an object 'iris' which is
a pandas dataframe with 150 rows and the following fields:
    iris['petal_length']
    iris['petal_width']
    iris['sepal_length']
    iris['sepal_width']
    iris['species']

Examples
--------
>>> from ibmdbpy.sampledata.iris import iris
'''

from __future__ import absolute_import
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from future import standard_library
standard_library.install_aliases()
from os.path import dirname, join
import pandas as pd

iris = pd.read_csv(join(dirname(__file__), 'iris.txt'))
