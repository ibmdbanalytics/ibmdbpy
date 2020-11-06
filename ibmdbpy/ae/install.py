#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------------
# Copyright (c) 2015, IBM Corp.
# All rights reserved.
#
# Distributed under the terms of the BSD Simplified License.
#
# The full license is in the LICENSE file, distributed with this software.
# -----------------------------------------------------------------------------

"""
In-Database User defined functions
"""
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import

import inspect
import textwrap
from builtins import dict
from future import standard_library
from ibmdbpy import IdaDataBase, IdaDataFrame

from ibmdbpy.ae import shaper, result_builder

standard_library.install_aliases()


class NZInstall(object):

    def __init__(self, package_name):
        """
        Constructor for install
        """
        # we need a way to directly access database rather than through dsn
        idadb = IdaDataBase('nzpy', 'admin', 'password')
        print(idadb)

        #idadf = IdaDataFrame(idadb, 'nzpy')
        self.package_name =package_name
        #self.table_name = idadf.internal_state.current_state

        # send the package name as dynamic variable to ae function
        columns_string = "'PACKAGE_TO_INSTALL= " + self.package_name + "'"
        #print("table name is " + self.table_name)
        # print(columns_string)

        ae_name ="nzpy..py_udtf_install"

        query = "select * from table with final (" + ae_name + "(" + columns_string + ")) "

        result = idadb.ida_query(query)
        return result






