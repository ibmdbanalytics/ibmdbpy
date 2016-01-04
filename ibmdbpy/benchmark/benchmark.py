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
Benchmark module. Provide a set of functions to compare the performance
of in-database functions and equivalent in-memory implementation
"""
from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals
from __future__ import absolute_import
from builtins import round
from builtins import range
from builtins import int
from future import standard_library
standard_library.install_aliases()
import os
from copy import deepcopy
from time import time
from functools import wraps

import pandas as pd

from ibmdbpy import IdaDataFrame
from ibmdbpy.utils import set_verbose
from ibmdbpy.exceptions import Error

def silent(function):
    """
    Decorate that silent the function it decorates,
    i.e SQL queries will not be printed.
    """
    @wraps(function)
    def wrapper(self, *args, **kwds):
        original_value = os.environ['VERBOSE']
        set_verbose(False)
        result = function(self, *args, **kwds)
        os.environ['VERBOSE'] = original_value
        return result
    return wrapper

def to_nK(dataframe, nKrow):
    """
    Create a version of the dataframe that has n Krows.

    Parameters
    ----------
    dataframe : DataFrame
        DataFrame to use as a basis
    nKrow : int
        Number of Krows the return dataframe should contain

    Returns
    -------
    DataFrame
        A Version of the inputed dataframe with n Krows

    Notes
    -----
        If the dataset df has less than 1000 rows, it will be imputed randomly
        with some existing rows, otherwise the first 1000 rows will be selected
        and then the dataset grows"""
    if nKrow < 1:
        raise ValueError("n should be an integer with minimum value 1")
    if not isinstance(nKrow, int):
        raise ValueError("n should be an integer with minimum value 1")
    def some(df, nrow):
        """Return n random rows from the given dataframe"""
        import numpy.random as random
        return df.ix[random.choice(range(len(df)), nrow)]
    if dataframe.shape[0] < 1000:
        df1K = pd.concat([dataframe, some(dataframe, 1000 - dataframe.shape[0])])
    else:
        df1K = dataframe.head(1000)
    df = deepcopy(df1K)
    if nKrow > 1:
        for x in range(1, nKrow):
            df = pd.concat([df, df1K])
    return df

def extend_dataset(df, n):
    """
    Extend a dataframe horizontaly by duplicating its columns n times
    """
    df_2 = deepcopy(df)
    df_out = deepcopy(df)
    while n > 0:
        df_2.columns = ["%s_extended_%s"%(column,n) for column in df.columns]
        df_out = pd.concat([df_out,df_2], axis = 1)
        n -= 1
    return df_out

###############################################################################
# benchmark class
###############################################################################

class Benchmark(object):
    """
    Benchmark class. Implement function to run comparision of speed between
    in-database function and same in-memory function. The Framework handle
    the size of the benchmark idadataframe and make it scale simultaneously
    with an identic dataframe.

    The name of the function to test should be the same for IdaDataFrame objects
    and Pandas DataFrame
    """
    def __init__(self, idadf, function_name, function_syntax, with_import = False, limit = 30):
        self.idadf = idadf
        self.df = idadf.as_dataframe()
        self.function_name = function_name
        self.function_syntax = function_syntax
        self.with_import = with_import
        self.limit = limit

    def initialize(self):
        """
        Initalize the benchmark : from the provided IdaDataFrame, create a
        new IdaDataFrame and an identic Pandas DataFrame that 1000 rows.
        """
        # Initialisation of in memory and in database dataframes
        print("*** Initializing benchmark to 1K, with command %s ***"%self.function_syntax)
        self.test_function()
        self.df = to_nK(self.df,1)
        self.idadf = self.idadf._idadb.as_idadataframe(self.df, "BENCHMARK", clear_existing = True)

        self.runtime_db_list = []
        self.runtime_m_list = []
        self.nrow_list = []

    def increment(self):
        """
        Increment simultaneously the benchmarking IdaDataFrame and Pandas DataFrame,
        with the same data. The datasets duplicates themselves by 2 untill they
        reach the limit of 256000 rows. From that moment only the first 256000
        are added to the datasets again and again. This is made to avoid having
        to upload to big quantity of data into dashDB at the same time.
        """
        print("*** Incrementing for next round ***")
        try:
            if len(self.df) < 256000:
                self.idadf._idadb.append(self.idadf, self.df)
                self.df = pd.concat([self.df,self.df])
            else:
                self.idadf._idadb.append(self.idadf, self.df[0:256000])
                self.df = pd.concat([self.df,self.df[0:256000]])
        except:
            print("An unexpected error occured. (Maybe out of memory)")
            raise

        if self.with_import:
            self.df.to_csv("tmp_csv.csv", index = False)

    @silent
    def run(self, initialize=True):
        """
        Run the benchmark automatically
        If initialize is set to True, the benchmark start from the beginning
        """
        def clean():
            try:
                os.remove("tmp_csv.csv")
            except:
                print("No temp csv to remove")

        if initialize:
            self.initialize()
        if self.with_import:
            self.df.to_csv("tmp_csv.csv", index = False)

        runtime_db = 0
        runtime_m = 0
        while (runtime_db < self.limit) & (runtime_m < self.limit):
            print("\n*** Benchmarking with %s rows ***"%len(self.df))

            start = time()
            # Do the operation 3 times and average to increase accuracy
            for i in [1,2,3]:
                if self.with_import:
                    self.idadf = IdaDataFrame(self.idadf._idadb, "BENCHMARK")
                exec("self.idadf.%s"%self.function_syntax)
            runtime_db = round((time() - start)/3.0,4)

            start = time()
            for i in [1,2,3]:
                if self.with_import:
                    self.df = pd.read_csv("tmp_csv.csv")
                exec("self.df.%s"%self.function_syntax)
            runtime_m = round((time() - start)/3.0,4)

            # Save the result
            self.nrow_list.append(len(self.df))
            self.runtime_db_list.append(runtime_db)
            self.runtime_m_list.append(runtime_m)

            print("Length of DataFrame : %s \t\t\t\t"%len(self.df) + "Length of IdaDataFrame : %s"%len(self.idadf))
            print("Runtime in-Memory : %s \t\t"%runtime_m + "Runtime in-Database : %s"%runtime_db)
            if((runtime_db < self.limit) & (runtime_m < self.limit)):
                try:
                    self.increment()
                except:
                    raise
                    clean()
                    return

        clean()

    def resume(self):
        """
        Resume the current benchmark
        Can be usefull for example if the benchmark stopped for an unexpected reasons
        (For example, it lost the connection)
        """
        return self.run(initialize=False)


    def visualize(self):
        """
        Create an interactive plot from the data gathered during benchmarking.
        To execute this function, you need to have installed the optional external
        package, "Bokeh". (pip install Bokeh)
        """
        try:
            from bokeh.plotting import figure, output_file, show
        except ImportError:
            raise Error("Bokeh is used by the benchmark framework for plotting"+
                        " the result. Please make sure it is installed in your"+
                        " system (pip install Bokeh)")
        legend = {}
        legend['y'] = "Seconds"
        legend['x'] = "Number of rows"
        legend['y1'] = "In-Memory runtime"
        legend['y2'] = "In-Database runtime"
        legend['command'] = self.function_name
        result = {}
        result['command'] = self.function_name
        result['x'] = self.nrow_list
        result['y1'] = self.runtime_m_list
        result['y2'] = self.runtime_db_list
        result['legend'] = legend
        import time
        filename = "%s_%s.html"%(self.function_name,time.strftime('%m-%d-%H-%M', time.localtime(time.time())))
        filename = filename.replace(" ", '')
        output_file(filename, title = filename)

        TOOLS='pan, box_zoom, wheel_zoom, save, box_select,crosshair,resize,reset'
        p = figure(width=800, height=800, x_axis_label = result['legend']['x'], y_axis_label = result['legend']['y'], title=result['command'], tools=TOOLS)
        p.line(result['x'], result['y1'], color="red", legend = result['legend']['y1'])
        p.line(result['x'], result['y2'], color="blue", legend = result['legend']['y2'])
        show(p)

    @silent
    def test_function(self):
        """
        Test if the provided function can execute both in the IdaDataFrame and Pandas DataFrame
        """
        try:
            exec("self.df." + self.function_syntax)
        except:
            print("Command is invalid for Pandas DataFrame")
            raise
        try:
            exec("self.idadf." + self.function_syntax)
        except:
            print("Command is invalid for IdaDataFrame")
            raise


###############################################################################
# Experiment class
###############################################################################

class Experiment(object):
    """
    Experiment class.
    Compare the result and runtime of two different functions in-database. 
    """
    def __init__(self, idadf, function1, function2, import_statement, f1_name='Function1', f2_name='Function2', expand = "duplicate_H", limit = 30, randomness = 3):
        self.import_statement = import_statement
        self.idadf = idadf
        self.df = idadf.as_dataframe()
        self.function1 = function1
        self.function2 = function2
        self.f1_name = f1_name
        self.f2_name = f2_name
        self.limit = limit
        self.expand = expand
        
        if "random" in self.expand:
            self.randomness = randomness
        else:
            self.randomness = ''
        
        self.argumentstr = "Experiment : %s %s %s"%(idadf.tablename, expand, randomness)

    def initialize(self):
        """
        Initalize the benchmark : from the provided IdaDataFrame, create a
        new IdaDataFrame and an identic Pandas DataFrame that 1000 rows.
        """
        # Initialisation of in memory and in database dataframes
        print("*** Initializing benchmark to 1K***")
        self.test_function()
        self.df = to_nK(self.df,1)
        self.idadf = self.idadf._idadb.as_idadataframe(self.df, "BENCHMARK", clear_existing = True)

        self.runtime_f1_list = []
        self.result_f1_list = []
        self.runtime_f2_list = []
        self.result_f2_list = []
        self.nrow_list = []
        self.ncol_list = []

    def increment(self):
        """
        Increment the BENCHMARK idadataframe
        """
        print("*** Incrementing for next round ***")
        try:
            if "duplicate" in self.expand:
                if "H" in self.expand:
                    if len(self.df) < 256000:
                        self.idadf._idadb.append(self.idadf, self.df)
                        self.df = pd.concat([self.df,self.df])
                    else:
                        self.idadf._idadb.append(self.idadf, self.df[0:256000])
                        #self.df = pd.concat([self.df,self.df[0:256000]])
                if "V" in self.expand:
                    if len(self.idadf.columns) <= 3:
                        for column in self.idadf.columns:
                            self.idadf._idadb.add_column(self.idadf, column = column)
                    else:
                        import random
                        columns = random.sample(list(self.idadf.columns), 3)
                        for column in columns:
                            self.idadf._idadb.add_column(self.idadf, column = column)
            elif "random" in self.expand:
                if "V" in self.expand:
                    print("Add random columns")
                    if len(self.idadf.columns) <= 3:
                        for column in self.idadf.columns:
                            self.idadf._idadb.add_column(self.idadf, ncat = self.randomness)
                    else:
                        import random
                        columns = random.sample(list(self.idadf.columns), 3)
                        for column in columns:
                            self.idadf._idadb.add_column(self.idadf, ncat = self.randomness)
                if "H" in self.expand:
                    print("Generate random dataframe from idadf")
                    self.df = self.idadf.head(10)
                    self.newdf = pd.DataFrame()
                    import random
                    tuplelist = []
                    while (len(tuplelist) < len(self.idadf))&(len(tuplelist) < 256000):
                        #tuplelist.append([random.choice(self.df[column]) for column in self.idadf.columns])
                        tuplelist.append([str(random.randint(1,self.randomness)) for column in self.idadf.columns])
                    self.newdf = pd.DataFrame(tuplelist)
                    self.newdf.columns = self.df.columns
                    #print(self.newdf.head())
                    self.idadf._idadb.append(self.idadf, self.newdf)
                    
        except:
            print("An unexpected error occured. (Maybe out of memory)")
            raise
            
        self.idadf.commit()

        #if self.with_import:
        #    self.df.to_csv("tmp_csv.csv", index = False)

    @silent
    def run(self, initialize=True):
        """
        Run the benchmark automatically
        If initialize is set to True, the benchmark start from the beginning
        """
        exec(self.import_statement)
        #def clean():
            #try:
            #    os.remove("tmp_csv.csv")
            #except:
            #    print("No temp csv to remove")

        if initialize:
            self.initialize()
        #if self.with_import:
        #    self.df.to_csv("tmp_csv.csv", index = False)

        runtime_f1 = 0
        runtime_f2 = 0
        while (runtime_f1 < self.limit) & (runtime_f2 < self.limit):
            print("\n*** Benchmarking with %s rows, %s cols ***"%(len(self.idadf),len(self.idadf.columns)))
            result = None
            start = time()
            # Do the operation 3 times and average to increase accuracy
            for i in [1,2,3]:
                #if self.with_import:
                #    self.idadf = IdaDataFrame(self.idadf._idadb, "BENCHMARK")
                exec("result = %s"%self.function1)
            runtime_f1 = round((time() - start)/3.0,4)
            result_f1 = result

            start = time()
            for i in [1,2,3]:
                #if self.with_import:
                #    self.df = pd.read_csv("tmp_csv.csv")
                #exec("result = self.idadf.%s"%self.function2)
                exec("result = %s"%self.function2)
            runtime_f2 = round((time() - start)/3.0,4)
            result_f2 = result
            

            # Save the result
            self.nrow_list.append(len(self.idadf))
            self.ncol_list.append(len(self.idadf.columns))
            self.runtime_f1_list.append(runtime_f1)
            self.runtime_f2_list.append(runtime_f2)
            self.result_f1_list.append(result_f1)
            self.result_f2_list.append(result_f2)

            print("Number of rows in the IdaDataFrame : %s"%len(self.idadf))
            print("Number of cols in the IdaDataFrame : %s"%len(self.idadf.columns))
            print("Runtime f1 : %s \t\t"%runtime_f1 + "Runtime f2 : %s"%runtime_f2)
            if((runtime_f1 < self.limit) & (runtime_f2 < self.limit)):
                try:
                    self.increment()
                except:
                    raise
                    #clean()
                    return

        #clean()

    def resume(self):
        """
        Resume the current benchmark
        Can be usefull for example if the benchmark stopped for an unexpected reasons
        (For example, it lost the connection)
        """
        return self.run(initialize=False)


    def visualize(self):
        """
        Create an interactive plot from the data gathered during benchmarking.
        To execute this function, you need to have installed the optional external
        package, "Bokeh". (pip install Bokeh)
        """
        try:
            from bokeh.plotting import figure, output_file, show
        except ImportError:
            raise Error("Bokeh is used by the benchmark framework for plotting"+
                        " the result. Please make sure it is installed in your"+
                        " system (pip install Bokeh)")
        legend = {}
        legend['y'] = "Seconds"
        
        legend['y1'] = self.f1_name
        legend['y2'] = self.f2_name
        #legend['command'] = self.function_name
        result = {}
        #result['command'] = self.function_name
        if self.expand in ["duplicate_H", "random_H"]:
            legend['x'] = "Number of rows"
            result['x'] = self.nrow_list
        elif self.expand in ["duplicate_V", "random_V"]:
            legend['x'] = "Number of cols"
            result['x'] = self.ncol_list
        elif self.expand in ["duplicate_HV", "random_HV"]:
            legend['x'] = "Number of cells"
            result['x'] = [x*y for x,y in zip(self.ncol_list,self.nrow_list)]
        result['y1'] = self.runtime_f1_list
        result['y2'] = self.runtime_f2_list
        result['legend'] = legend
        import time
        filename = "%s_%s.html"%("exp",time.strftime('%m-%d-%H-%M', time.localtime(time.time())))
        filename = filename.replace(" ", '')
        output_file(filename, title = filename)

        TOOLS='pan, box_zoom, wheel_zoom, save, box_select,crosshair,resize,reset'
        p = figure(width=800, height=800, x_axis_label = result['legend']['x'], y_axis_label = result['legend']['y'], title="%s"%self.argumentstr, tools=TOOLS)
        p.line(result['x'], result['y1'], color="red", legend = result['legend']['y1'])
        p.line(result['x'], result['y2'], color="blue", legend = result['legend']['y2'])
        show(p)

    @silent
    def test_function(self):
        """
        Test if the both functions can be executed
        """
        exec(self.import_statement)
        try:
            #func1 = ''
            execute = "func1 = %s"%self.function1
            exec(execute)
            print("Execution of %s (%s)"%(self.f1_name,execute))
            #print(func1)
        except:
            print("Function1 is invalid for IdaDataFrame")
            raise
        try:
            #func2 = ''
            execute = "func2 = %s"%self.function2
            exec(execute)
            print("Execution of %s (%s)"%(self.f2_name,execute))
            #print(func2)
        except:
            print("Function2 is invalid for IdaDataFrame")
            raise