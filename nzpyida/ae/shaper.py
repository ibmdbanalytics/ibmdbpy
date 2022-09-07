import inspect
import textwrap


def get_base_shaper_tapply(columns, fun_name, output_signature):
    output_signature_str = get_base_shaper(output_signature)




    # output_signature_final = inspect.cleandoc(output_signature_str)
    code_string = """import nzae
                            import pandas as pd
                            class BaseShaperUdtf(nzae.Ae):
                                 def _runUdtf(self):
                                   
                                   df = pd.DataFrame(self.__iter__(), columns=""" + str(columns) + """ )
                                   value = self.""" + fun_name + """(df)
                                   #rows_list.append(str(self.getDatasliceId())+str(self.getHardwareId()))  
                                  

                                 def _runShaper(self):
                                    
                                    """ + textwrap.indent(output_signature_str, '                      ') + """
                                    




                                   """

    code_string = code_string.replace("'", "''")
    return inspect.cleandoc(code_string)

def get_base_shaper_groupedapply(columns, fun_name, output_signature):
    output_signature_str = get_base_shaper(output_signature)




        # output_signature_final = inspect.cleandoc(output_signature_str)
    code_string = """import nzae
                        import pandas as pd
                        class BaseShaperUdtf(nzae.Ae):
                             def _runUdtf(self):
                               rows_list=[]
                               for row in self:
                                  row_number = row[0]
                                  count = row[1]

                                  if row_number==1:
                                    rows_list=[]
                                  rows_list.append(row[2:])
                                  #rows_list.append(str(self.getDatasliceId())+str(self.getHardwareId()))  
                                  if row_number==count:                                                              
                                   df = pd.DataFrame(rows_list, columns=""" + str(columns) + """ )
                                   value = self.""" + fun_name + """(df)
                                   #self.output(value)
                                   #self.output(len(rows_list))




                             def _runShaper(self):
                               
                                     """ + textwrap.indent(output_signature_str, '                      ') + """



                               """

    code_string = code_string.replace("'", "''")
    return inspect.cleandoc(code_string)

def get_base_shaper(output_signature):
    output_signature_str = ""


    for column in output_signature:

        if output_signature[column] == 'int' :
            column_val = 'self.DATA_TYPE__INT32'

            output_signature_str += """
                                                       self.addOutputColumn('""" + column + """',""" + column_val + """) """
        if output_signature[column] == 'int64':
            column_val = 'self.DATA_TYPE__INT64'

            output_signature_str += """
                                                       self.addOutputColumn('""" + column + """',""" + column_val + """) """

        if output_signature[column] == 'bool' :
            column_val = 'self.DATA_TYPE__BOOL'
            output_signature_str += """
                                                       self.addOutputColumn('""" + column + """',""" + column_val + """) """

        if output_signature[column] == 'datetime' or  output_signature[column] == 'date':

            
            column_val = 'self.DATA_TYPE__DATE'
            output_signature_str += """
                                                       self.addOutputColumn('""" + column + """',""" + column_val + """) """

        if output_signature[column] == 'float' or output_signature[column] == 'float64':
            column_val = 'self.DATA_TYPE__FLOAT'
            output_signature_str += """
         
                                                       self.addOutputColumn('""" + column + """',""" + column_val + """) """


        if output_signature[column] == 'double':
            column_val = 'self.DATA_TYPE__DOUBLE'
            output_signature_str += """
                                                       self.addOutputColumn('""" + column + """',""" + column_val + """) """
        if output_signature[column] == 'str' or output_signature[column] == 'object':
            column_val = 'self.DATA_TYPE__VARIABLE'
            output_signature_str += """
                                                       self.addOutputColumnString('""" + column + """',""" + column_val + """,1000) """
    return output_signature_str


def get_base_shaper_apply(columns, fun_name, output_signature):

    output_signature_str = get_base_shaper(output_signature)
    # output_signature_final = inspect.cleandoc(output_signature_str)
    code_string = """import nzae
                            import pandas as pd
                            class BaseShaperUdtf(nzae.Ae):
                                 def _runUdtf(self):
                                  
                                   for row in self:
                                     row = self.apply_fun(row)  
                                   

                                 def _runShaper(self):
                                   """ + textwrap.indent(output_signature_str, '                      ') + """




                                   """

    code_string = code_string.replace("'", "''")
    return inspect.cleandoc(code_string)


def get_base_shaper_install(output_signature, package_name):
    output_signature_str = get_base_shaper(output_signature)
    # output_signature_final = inspect.cleandoc(output_signature_str)
    code_string = """import nzae
                            
                            class BaseShaperUdtf(nzae.Ae):
                                 def _runUdtf(self):

                                    import subprocess
                                    cwd = os.getcwd()
                                    
                                    for row in self:
                                                                
                                        
                                        res = subprocess.call([\'sh', \'/nz/export/ae/adapters/python3x/11/lib/installPackage.sh',  """+"\'"+package_name+"\'"+"""])
                                       
                                        self.output(res)
                                       
                                    


                                 def _runShaper(self):
                                   """ + textwrap.indent(output_signature_str, '       ') + """




                                   """

    code_string = code_string.replace("'", "''")
    return inspect.cleandoc(code_string)








