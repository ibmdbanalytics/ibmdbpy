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
This module provides a data set containing (fake) information about passengers
of the titanic. This dataset was used in a famous Kaggle data science competition.
As user, you can import the object 'titanic' which is a pandas dataframe with
891 rows and the following fields:
    titanic['PASSENGERID']
    titanic['SURVIVED']
    titanic['PCLASS']
    titanic['NAME']
    titanic['SEX']
    titanic['AGE']
    titanic['SIBSP']
    titanic['PARCH']
    titanic['TICKET']
    titanic['FARE']
    titanic['CABIN']
    titanic['EMBARKED']

Examples
--------
>>> from ibmdbpy.sampledata.titanic import titanic
'''

from __future__ import absolute_import
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from future import standard_library
standard_library.install_aliases()
from os.path import dirname, join
import pandas as pd

titanic = pd.read_csv(join(dirname(__file__), 'titanic.txt'), sep = '|')
titanic.name = "titanic"