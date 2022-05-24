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

from nzpyida.ae import shaper, result_builder

standard_library.install_aliases()


class NZFunGroupedApply(object):

    def __init__(self, df, index, output_signature, output_table=None, fun_ref=None,  code_str=None,  fun_name=None, columns=None, merge_output_with_df=False, id='ID'):
        """
        Constructor for tapply
        """
        self.table_name = df.internal_state.current_state
        self.df = df
        self.db = df._idadb
        self.fun = fun_ref
        self.fun_name = fun_name
        self.id = id

        self.code_str= code_str
        self.merge_output= merge_output_with_df

        self.index = index
        self.output_table = output_table
        self.output_signature = output_signature
        self.parallel = True

        # convert the columns index object to a list
        if columns:
            self.columns=columns
        else :
            self.columns = self.df.columns.tolist()


    def get_result(self):
        if self.code_str and self.fun_name is None:
            raise Exception("fun_name is required")
        # we need a comma separated string of column values
        #columns_string = ",".join(self.columns)
        # print(columns_string)



        if self.code_str:
            code_string = self.build_udtf_shaper_ae_code_fun_as_str(self.code_str, self.fun_name, self.columns,
                                                                    self.output_signature)
        else:
            code_string = self.build_udtf_shaper_ae_code_fun_as_ref(self.fun, self.columns, self.output_signature)



        compressions_string = result_builder.compress_columns_string(self.columns)

        query = ""
        # send the code as dynamic variable to ae function
        columns_string = compressions_string + ",'CODE_TO_EXECUTE=" + "\"" + code_string + "\"" + "'"
        if self.parallel is False:
            ae_name = "nzpyida..py_udtf_host"
            query = "select ae_output.* from " + \
                    " (select * from " + self.db_name + ") as input_t" + \
                    ", table with final (" + ae_name + columns_string + ")) as ae_output"
        else:
            ae_name = "nzpyida..py_udtf_any"
            query = "(select row_number() over (partition by " + self.index + " order by " + self.index + ") as  rn,  count(*)  over (partition" + \
                    " by " + self.index + ") as   ct,    " + self.table_name + ".*   from " + self.table_name + ") as input_t"
            query = "select ae_output.* from " + \
                    query + \
                    ", table with final (" + ae_name + "(rn,ct," + columns_string + ")) as ae_output"
        result = result_builder.build_result(self.output_table, self.merge_output, self.db, self.df,
                                             self.output_signature, self.table_name, query, self.id)
        return result



    def build_udtf_shaper_ae_code_fun_as_str(self, code_str, fun_name, columns, output_signature):
        # we need extra single quotes for correct escaping

        fun_code = code_str
        fun_code = fun_code.replace("'", "''")


        if self.parallel is False:
            base_code = self.get_base_shaper_host(columns, fun_name, output_signature)
        else:
            base_code = shaper.get_base_shaper_groupedapply(columns, fun_name, output_signature)

        run_string = textwrap.dedent(""" BaseShaperUdtf.run()""")

        final_code = base_code + "\n" + textwrap.indent(fun_code, '     ')
        final_code = final_code+"\n"+run_string



        return inspect.cleandoc(final_code)




    def build_udtf_shaper_ae_code_fun_as_ref(self, fun, columns, output_signature):


        return self.build_udtf_shaper_ae_code_fun_as_str(inspect.getsource(fun).lstrip(), fun.__name__, columns,
                                                         output_signature)




    def get_base_shaper_host(self, columns, fun_name, output_signature):

        output_signature_str = shaper.get_base_shaper(output_signature)





        code_string = """               import nzae
                                        import pandas as pd
                                        class BaseShaperUdtf(nzae.Ae):
                                             def _runUdtf(self):
                                               rows_list=[]
                                               for row in self:
                                                 rows_list.append(row)
                                                 #rows_list.append(str(self.getDatasliceId())+str(self.getHardwareId()))  
                                               df = pd.DataFrame(rows_list, columns=""" + str(columns) + """ )
                                               value = self.""" + fun_name + """(df)
                                               self.output(value)
                                               #self.output(str(len(rows_list)), str(rows_list[12000]))

                                             def _runShaper(self):
                                               """ + textwrap.indent(output_signature_str, '                      ') + """




                                               """

        code_string = code_string.replace("'", "''")
        return inspect.cleandoc(code_string)


