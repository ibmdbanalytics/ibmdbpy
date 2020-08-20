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
from ibmdbpy import IdaDataFrame

standard_library.install_aliases()


class NZFunApply(object):

    def __init__(self, df, fun, output_signature, output_table=None):
        """
        Constructor for tapply
        """
        self.db_name = df.internal_state.current_state
        self.df = df
        self.db = df._idadb
        self.fun = fun
        self.output_table = output_table
        self.output_signature =output_signature



        # convert the columns index object to a list
        self.columns = self.df.columns.tolist()

    def get_result(self):
        # we need a comma separated string of column values
        columns_string = ",".join(self.columns)
        # print(columns_string)

        # get the default ae class coupled with client ml code
        # code_string = self.build_udtf_ae_code(self.fun, self.columns)
        code_string = self.build_udtf_shaper_ae_code(self.fun, self.columns, self.output_signature)


        # send the code as dynamic variable to ae function
        columns_string = columns_string + ",'CODE_TO_EXECUTE=" + "\"" + code_string + "\"" + "'"

        print("db name is " + self.db_name)
        # print(columns_string)

        ae_name ="py_udtf_any"

        query = "select ae_output.* from " + \
                " (select * from " + self.db_name + ") as input_t" + \
                ", table with final (" + ae_name + "(" + columns_string + ")) as ae_output"


        if self.output_table:
            if self.db.exists_table(self.output_table):
              create_string = "insert into "+self.output_table+" "
            else:
              create_string = "create table " + self.output_table + " as "
            query = create_string+query

            try:
             result = self.df.ida_query(query, autocommit=True)
            except :
             raise

            idadf = IdaDataFrame(self.db, self.output_table)
            df = idadf.as_dataframe()
            return df
        result = self.df.ida_query(query)
        return result


    def build_udtf_ae_code(self, fun, columns):
        # we need extra single quotes for correct escaping

        fun_code = inspect.getsource(fun)
        fun_code = fun_code.replace("'", "''")

        base_code = self.get_base_udtf(columns, fun)

        run_string = textwrap.dedent(""" BaseUdtf.run()""")

        final_code = base_code + "\n" + textwrap.indent(fun_code, '    ') \
                     + run_string

        print_string = """
       
        print(3+4)
        """

        return inspect.cleandoc(final_code)


    def build_udtf_shaper_ae_code(self, fun, columns, output_signature):
        # we need extra single quotes for correct escaping

        fun_code = inspect.getsource(fun)
        fun_code = fun_code.replace("'", "''")

        base_code = self.get_base_shaper(columns, fun, output_signature)

        run_string = textwrap.dedent(""" BaseShaperUdtf.run()""")

        final_code = base_code + "\n" + textwrap.indent(fun_code, '     ') \
                     + run_string


        print_string = """
        print(3+4)

        """

        return inspect.cleandoc(final_code)

    def get_base_udtf(self, columns, fun):
        fun_name = fun.__name__

        code_string = """
                import nzae
                import pandas as pd
                class BaseUdtf(nzae.Ae):
                    def _runUdtf(self):
                       rows_list=[]
                       for row in self:
                         rows_list.append(row)
                       df = pd.DataFrame(rows_list, columns=""" + str(columns) + """ )
                       self.""" + fun_name + """(df)"""

        code_string = code_string.replace("'", "''")

        return inspect.cleandoc(code_string)

    def get_base_shaper(self, columns, fun, output_signature):
        fun_name = fun.__name__
        output_signature_str = ""

        for i in range(len(output_signature)):
            column_signature = output_signature[i]
            col_sig_list = column_signature.split('=')
            if col_sig_list[1]=='int':
                col_sig_list[1]= 'self.DATA_TYPE__INT32'
                output_signature_str += """
                            self.addOutputColumn('""" + col_sig_list[0] + """',""" + col_sig_list[
                    1] + """) """
            if col_sig_list[1] == 'float':
                col_sig_list[1] = 'self.DATA_TYPE__FLOAT'
                output_signature_str += """
                            self.addOutputColumn('""" + col_sig_list[0] + """',""" + col_sig_list[
                    1] + """) """
            if col_sig_list[1] == 'double':
                col_sig_list[1] = 'self.DATA_TYPE__DOUBLE'
                output_signature_str += """
                            self.addOutputColumn('""" + col_sig_list[0] + """',""" + \
                                        col_sig_list[
                                            1] + """) """
            if col_sig_list[1] == 'str':
                col_sig_list[1] = 'self.DATA_TYPE__VARIABLE'
                output_signature_str += """
                            self.addOutputColumnString('""" + col_sig_list[
                    0] + """',""" + \
                                        col_sig_list[
                                            1] + """,1000) """




        #output_signature_final = inspect.cleandoc(output_signature_str)
        code_string ="""import nzae
                        import pandas as pd
                        class BaseShaperUdtf(nzae.Ae):
                             def _runUdtf(self):
                               for row in self :
                                 row = self.""" + fun_name + """(row)
                                 self.output(row)
                               
                               
                             def _runShaper(self):
                               """   + textwrap.indent(output_signature_str, '                      ') + """
                                                          
        
                             
                               
                               """

        code_string = code_string.replace("'", "''")
        return inspect.cleandoc(code_string)
