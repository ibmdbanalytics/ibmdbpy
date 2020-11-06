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

from ibmdbpy.ae import shaper, result_builder

standard_library.install_aliases()


class NZFunTApply(object):

    def __init__(self, df,  parallel,  output_signature=None, output_table=None, code_str=None, fun_ref=None, fun_name=None, merge_output_with_df=False):
        """
        Constructor for tapply
        """
        self.table_name = df.internal_state.current_state
        self.df = df
        self.db = df._idadb
        self.fun = fun_ref
        self.fun_name =fun_name
        self.code_str = code_str
        self.output_table = output_table
        self.output_signature = output_signature
        self.parallel =parallel
        self.merge_output = merge_output_with_df

        # convert the columns index object to a list
        self.columns = self.df.columns.tolist()





    def get_result(self):
        # we need a comma separated string of column values
        columns_string = ",".join(self.columns)
        # print(columns_string)

        # get the default ae class coupled with client ml code
        # code_string = self.build_udtf_ae_code(self.fun, self.columns)

        if self.code_str:
         code_string = self.build_udtf_shaper_ae_code_fun_as_str(self.code_str, self.fun_name, self.columns, self.output_signature)
        else :
         code_string = self.build_udtf_shaper_ae_code_fun_as_ref(self.fun, self.columns, self.output_signature)


        # send the code as dynamic variable to ae function
        columns_string = columns_string + ",'CODE_TO_EXECUTE=" + "\"" + code_string + "\"" + "'"

        print("table name is " + self.table_name)
        # print(columns_string)
        if not self.parallel:
            ae_name = "nzpy..py_udtf_host"
        else:
            ae_name ="nzpy..py_udtf_any"

        query = "select ae_output.* from " + \
                " (select * from " + self.table_name + ") as input_t" + \
                ", table with final (" + ae_name + "(" + columns_string + ")) as ae_output"

        # print(query)
        result = result_builder.build_result(self.output_table, self.merge_output, self.db, self.df,
                                             self.output_signature, self.table_name, query)
        return result





    def build_udtf_shaper_ae_code_fun_as_ref(self, fun, columns, output_signature):
        # we need extra single quotes for correct escaping

        fun_code = inspect.getsource(fun)
        fun_code = fun_code.replace("'", "''")
        fun_name = fun.__name__


        if output_signature is None:
            base_code =self.get_base_default(columns, fun_name)
        else:
            base_code = shaper.get_base_shaper_tapply(columns, fun_name, output_signature)

        run_string = textwrap.dedent(""" BaseShaperUdtf.run()""")

        final_code = base_code + "\n" + textwrap.indent(fun_code, '     ')
        final_code = final_code+"\n"+ run_string


        print_string = """
        print(3+4)

        """

        return inspect.cleandoc(final_code)

    def build_udtf_shaper_ae_code_fun_as_str(self, code_str, fun_name, columns, output_signature):
        # we need extra single quotes for correct escaping

        fun_code = code_str

        fun_code = fun_code.replace("'", "''")

        base_code = shaper.get_base_shaper_tapply(columns, fun_name, output_signature)



        run_string = textwrap.dedent(""" BaseShaperUdtf.run()""")
        final_code = base_code + "\n" + textwrap.indent(fun_code, '     ')
        final_code = final_code + "\n" + run_string





        return inspect.cleandoc(final_code)



