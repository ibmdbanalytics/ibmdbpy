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
Test module for IdaDataFrameObjects
"""
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from builtins import round
from builtins import zip
from future import standard_library
standard_library.install_aliases()

import pandas
import pytest

from ibmdbpy.feature_selection import pearson 
from ibmdbpy.feature_selection import spearman
from ibmdbpy.feature_selection import ttest
from ibmdbpy.feature_selection import chisquared
from ibmdbpy.feature_selection import gini, gini_pairwise
from ibmdbpy.feature_selection import entropy
from ibmdbpy.feature_selection import info_gain, gain_ratio, su

# Test symmetry

class Test_PearsonCorrelation(object):

    def test_pearson_default(self, idadf):
        data = idadf._table_def() 
        columns = list(data.loc[data['VALTYPE'] == "NUMERIC"].index)
        if len(columns) > 1:
            result = pearson(idadf, features = columns)
            assert(isinstance(result, pandas.core.frame.DataFrame))
            assert(len(result.columns) == len(columns))
            assert(len(result.index) == len(columns))
            result2 = pearson(idadf)
            assert(all(result == result2))
            result = result.fillna(0) # np.nan values are not equal when compared
            assert(all(result == result.T)) # symmetry
                
    def test_pearson_valueError(self, idadf):
        if len(idadf.columns) > 0:
            with pytest.raises(ValueError):
                pearson(idadf, features = idadf.columns[0])
            with pytest.raises(ValueError):
                pearson(idadf, target= idadf.columns[0], features = idadf.columns[0])
                
    def test_pearson_TypeError(self, idadf):
        data = idadf._table_def() 
        columns = list(data.loc[data['VALTYPE'] != "NUMERIC"].index)
        if len(columns) > 1:
            with pytest.raises(TypeError):
                pearson(idadf, features = columns)
                
    def test_pearson_one_target(self, idadf):
        data = idadf._table_def() 
        columns = list(data.loc[data['VALTYPE'] == "NUMERIC"].index)
        if len(columns) > 1:
            result = pearson(idadf, target = columns[0])
            assert(isinstance(result, pandas.core.series.Series))
            assert(len(result) == (len(columns)-1))
            
    def test_pearson_multiple_target(self, idadf):
        data = idadf._table_def() 
        columns = list(data.loc[data['VALTYPE'] == "NUMERIC"].index)
        if len(columns) > 1:
            result = pearson(idadf, target = [columns[0],columns[1]])
            assert(isinstance(result, pandas.core.frame.DataFrame))
            assert(len(result.columns) == 2)
            assert(len(result.index) == len(columns))
            
    def test_pearson_one_target_one_feature(self, idadf):
        data = idadf._table_def() 
        columns = list(data.loc[data['VALTYPE'] == "NUMERIC"].index)
        if len(columns) > 1:
            result = pearson(idadf, target = columns[0], features=[columns[1]])
            assert(isinstance(result, float))
            result2 = pearson(idadf, target = columns[1], features=[columns[0]])
            assert(round(result,3) == round(result2,3)) # symmetry
    
class Test_SpearmanRankCorrelation(object):

    def test_spearman_default(self, idadf):
        data = idadf._table_def() 
        columns = list(data.loc[data['VALTYPE'] == "NUMERIC"].index)
        if len(columns) > 1:
            result = spearman(idadf, features = columns)
            assert(isinstance(result, pandas.core.frame.DataFrame))
            assert(len(result.columns) == len(columns))
            assert(len(result.index) == len(columns))
            result2 = spearman(idadf)
            assert(all(result == result2))
            result = result.fillna(0) # np.nan values are not equal when compared
            assert(all(result == result.T)) # symmetry
                
    def test_spearman_valueError(self, idadf):
        if len(idadf.columns) > 0:
            with pytest.raises(ValueError):
                spearman(idadf, features = idadf.columns[0])
            with pytest.raises(ValueError):
                spearman(idadf, target= idadf.columns[0], features = idadf.columns[0])
                
    def test_spearman_TypeError(self, idadf):
        data = idadf._table_def() 
        columns = list(data.loc[data['VALTYPE'] != "NUMERIC"].index)
        if len(columns) > 1:
            with pytest.raises(TypeError):
                spearman(idadf, features = columns)
                
    def test_spearman_one_target(self, idadf):
        data = idadf._table_def() 
        columns = list(data.loc[data['VALTYPE'] == "NUMERIC"].index)
        if len(columns) > 1:
            result = spearman(idadf, target = columns[0])
            assert(isinstance(result, pandas.core.series.Series))
            assert(len(result) == (len(columns)-1))
            
    def test_spearman_multiple_target(self, idadf):
        data = idadf._table_def() 
        columns = list(data.loc[data['VALTYPE'] == "NUMERIC"].index)
        if len(columns) > 1:
            result = spearman(idadf, target = [columns[0],columns[1]])
            assert(isinstance(result, pandas.core.frame.DataFrame))
            assert(len(result.columns) == 2)
            assert(len(result.index) == len(columns))
            
    def test_spearman_one_target_one_feature(self, idadf):
        data = idadf._table_def() 
        columns = list(data.loc[data['VALTYPE'] == "NUMERIC"].index)
        if len(columns) > 1:
            result = spearman(idadf, target = columns[0], features=[columns[1]])
            assert(isinstance(result, float))
            result2 = spearman(idadf, target = columns[1], features=[columns[0]])
            assert(round(result,3) == round(result2,3)) # symmetry
    
class Test_Tstatistics(object):

    def test_ttest_default(self, idadf):
        data = idadf._table_def() 
        columns = list(data.loc[data['VALTYPE'] == "NUMERIC"].index)
        if len(columns) > 1:
            result = ttest(idadf, features = columns)
            assert(isinstance(result, pandas.core.frame.DataFrame))
            assert(len(result.columns) == len(columns))
            assert(len(result.index) == len(columns))
            result = result.fillna(0) # np.nan values are not equal when compared
            result = result[result.index]
            assert(all(result != result.T)) # asymmetry
            
    def test_ttest_valueError(self, idadf):
        data = idadf._table_def() 
        columns = list(data.loc[data['VALTYPE'] != "NUMERIC"].index)
        if len(columns) > 0: # Raise no numerical features
            with pytest.raises(ValueError):
                ttest(idadf, features = columns)
        if len(idadf.columns) > 0: 
            with pytest.raises(ValueError): # Cannot compute correlation coefficients of only one column (...), need at least 2
                ttest(idadf, features = idadf.columns[0])
            with pytest.raises(ValueError): # The correlation value of two same columns is always maximal
                ttest(idadf, target= idadf.columns[0], features = idadf.columns[0])
                
    def test_ttest_TypeError(self, idadf):
        data = idadf._table_def() 
        columns = list(data.loc[data['VALTYPE'] != "NUMERIC"].index)
        if len(columns) > 1:
            with pytest.raises(TypeError):
                ttest(idadf, features = columns)
                
    def test_ttest_one_target(self, idadf):
        data = idadf._table_def() 
        columns = list(data.loc[data['VALTYPE'] == "NUMERIC"].index)
        if len(columns) > 1:
            result = ttest(idadf, target = columns[0])
            assert(isinstance(result, pandas.core.series.Series))
            assert(len(result) == (len(columns)-1))
            
    def test_ttest_multiple_target(self, idadf):
        data = idadf._table_def() 
        columns = list(data.loc[data['VALTYPE'] == "NUMERIC"].index)
        if len(columns) > 1:
            result = ttest(idadf, target = [columns[0],columns[1]])
            assert(isinstance(result, pandas.core.frame.DataFrame))
            assert(len(result.columns) == 2)
            assert(len(result.index) == len(columns))
            
    def test_ttest_one_target_one_feature(self, idadf):
        data = idadf._table_def() 
        columns = list(data.loc[data['VALTYPE'] == "NUMERIC"].index)
        if len(columns) > 1:
            result = ttest(idadf, target = columns[0], features=[columns[1]])
            assert(isinstance(result, float))
            
    
class Test_Chisquared(object):
    
    def test_chisquared_default(self, idadf):
        if len(idadf.columns) > 1:
            result = chisquared(idadf, features = idadf.columns)
            assert(isinstance(result, pandas.core.frame.DataFrame))
            assert(len(result.columns) == len(idadf.columns))
            assert(len(result.index) == len(idadf.columns))
            result2 = chisquared(idadf)
            assert(all(result == result2))
            result = result.fillna(0) # np.nan values are not equal when compared
            assert(all(result == result.T)) # symmetry
            
    def test_chisquared_valueError(self, idadf):
        if len(idadf.columns) > 0: 
            with pytest.raises(ValueError): # Cannot compute correlation coefficients of only one column (...), need at least 2
                chisquared(idadf, features = idadf.columns[0])
            with pytest.raises(ValueError): # The correlation value of two same columns is always maximal
                chisquared(idadf, target= idadf.columns[0], features = idadf.columns[0])
                
    def test_chisquared_one_target(self, idadf):
        if len(idadf.columns) > 1:
            result = chisquared(idadf, target = idadf.columns[0])
            assert(isinstance(result, pandas.core.series.Series))
            assert(len(result) == (len(idadf.columns)-1))
            
    def test_chisquared_multiple_target(self, idadf):
        if len(idadf.columns) > 1:
            result = chisquared(idadf, target = [idadf.columns[0],idadf.columns[1]])
            assert(isinstance(result, pandas.core.frame.DataFrame))
            assert(len(result.columns) == 2)
            assert(len(result.index) == len(idadf.columns))
            
    def test_chisquared_one_target_one_feature(self, idadf):
        if len(idadf.columns) > 1:
            result = chisquared(idadf, target = idadf.columns[0], features=[idadf.columns[1]])
            assert(isinstance(result, float))
            result2 = chisquared(idadf, target = idadf.columns[1], features=[idadf.columns[0]])
            assert(round(result,3) == round(result2,3)) # symmetry
    
class Test_Gini_Index(object):

    def test_gini_default(self, idadf):
        if len(idadf.columns) > 1:
            result = gini(idadf, features = idadf.columns)
            assert(isinstance(result, pandas.core.series.Series))
            assert(len(result.index) == len(idadf.columns))
            result2 = gini(idadf)
            assert(all(result == result2))
            
    def test_gini_multiple_columns(self, idadf):
        if len(idadf.columns) > 1:
            result = gini(idadf, features = [idadf.columns[0],idadf.columns[1]])
            assert(isinstance(result, pandas.core.series.Series))
            assert(len(result) == 2)
            
    def test_gini_one_column(self, idadf):
        if len(idadf.columns) >= 1:
            result = gini(idadf, features = idadf.columns[0])
            assert(isinstance(result, float))
            
class Test_Gini_Pairwise(object):

    def test_gini_pairwise_default(self, idadf):
        if len(idadf.columns) > 1:
            result = gini_pairwise(idadf, features = idadf.columns)
            assert(isinstance(result, pandas.core.frame.DataFrame))
            assert(len(result.columns) == len(idadf.columns))
            assert(len(result.index) == len(idadf.columns))
            result2 = gini_pairwise(idadf)
            assert(all(result == result2)) 
            result = result.fillna(0) # np.nan values are not equal when compared
            assert(any(result != result.T)) # asymmetry
            
    def test_gini_pairwise_valueError(self, idadf):
        if len(idadf.columns) > 0: 
            with pytest.raises(ValueError): # Cannot compute correlation coefficients of only one column (...), need at least 2
                gini_pairwise(idadf, features = idadf.columns[0])
            with pytest.raises(ValueError): # The correlation value of two same columns is always maximal
                gini_pairwise(idadf, target= idadf.columns[0], features = idadf.columns[0])
                
    def test_gini_pairwise_one_target(self, idadf):
        if len(idadf.columns) > 1:
            result = gini_pairwise(idadf, target = idadf.columns[0])
            assert(isinstance(result, pandas.core.series.Series))
            assert(len(result) == (len(idadf.columns)-1))
            
    def test_gini_pairwise_multiple_target(self, idadf):
        if len(idadf.columns) > 1:
            result = gini_pairwise(idadf, target = [idadf.columns[0],idadf.columns[1]])
            assert(isinstance(result, pandas.core.frame.DataFrame))
            assert(len(result.columns) == 2)
            assert(len(result.index) == len(idadf.columns))
            
    def test_gini_pairwise_one_target_one_feature(self, idadf):
        if len(idadf.columns) > 1:
            result = gini_pairwise(idadf, target = idadf.columns[0], features=[idadf.columns[1]])
            assert(isinstance(result, float))
    
class Test_Entropy(object):

    def test_entropy_default(self, idadf):
        if len(idadf.columns) > 1:
            result = entropy(idadf)
            assert(isinstance(result, pandas.core.series.Series))
            assert(len(result.index) == len(idadf.columns))
            
    def test_entropy_multiple_columns(self, idadf):
        if len(idadf.columns) > 1:
            result = entropy(idadf, target = [idadf.columns[0],idadf.columns[1]])
            assert(isinstance(result, float))
            
    def test_entropy_one_column(self, idadf):
        if len(idadf.columns) >= 1:
            result = entropy(idadf, target = idadf.columns[0])
            assert(isinstance(result, float))
    
class Test_Information_Gain(object):

    def test_info_gain_default(self, idadf):
        if len(idadf.columns) > 1:
            result = info_gain(idadf, features = idadf.columns)
            assert(isinstance(result, pandas.core.frame.DataFrame))
            assert(len(result.columns) == len(idadf.columns))
            assert(len(result.index) == len(idadf.columns))
            result2 = info_gain(idadf)
            assert(all(result == result2))
            result = result.fillna(0) # np.nan values are not equal when compared
            assert(all(result == result.T)) # symmetry
            
    def test_info_gain_valueError(self, idadf):
        if len(idadf.columns) > 0: 
            with pytest.raises(ValueError): # Cannot compute correlation coefficients of only one column (...), need at least 2
                info_gain(idadf, features = idadf.columns[0])
            with pytest.raises(ValueError): # The correlation value of two same columns is always maximal
                info_gain(idadf, target= idadf.columns[0], features = idadf.columns[0])
                
    def test_info_gain_one_target(self, idadf):
        if len(idadf.columns) > 1:
            result = info_gain(idadf, target = idadf.columns[0])
            assert(isinstance(result, pandas.core.series.Series))
            assert(len(result) == (len(idadf.columns)-1))
            
    def test_info_gain_multiple_target(self, idadf):
        if len(idadf.columns) > 1:
            result = info_gain(idadf, target = [idadf.columns[0],idadf.columns[1]])
            assert(isinstance(result, pandas.core.frame.DataFrame))
            assert(len(result.columns) == 2)
            assert(len(result.index) == len(idadf.columns))
            
    def test_info_gain_one_target_one_feature(self, idadf):
        if len(idadf.columns) > 1:
            result = info_gain(idadf, target = idadf.columns[0], features=[idadf.columns[1]])
            assert(isinstance(result, float))
            result2 = info_gain(idadf, target = idadf.columns[1], features=[idadf.columns[0]])
            assert(round(result,3) == round(result2,3)) # symmetry
    
class Test_Symmetric_Gain_Ratio(object):

    def test_sym_gain_ratio_default(self, idadf):
        if len(idadf.columns) > 1:
            result = gain_ratio(idadf, features = idadf.columns, symmetry=True)
            assert(isinstance(result, pandas.core.frame.DataFrame))
            assert(len(result.columns) == len(idadf.columns))
            assert(len(result.index) == len(idadf.columns))
            result2 = gain_ratio(idadf, symmetry=True)
            assert(all(result == result2))
            result = result.fillna(0) # np.nan values are not equal when compared
            assert(all(result == result.T)) # symmetry
            
    def test_sym_gain_ratio_valueError(self, idadf):
        if len(idadf.columns) > 0: 
            with pytest.raises(ValueError): # Cannot compute correlation coefficients of only one column (...), need at least 2
                gain_ratio(idadf, features = idadf.columns[0], symmetry=True)
            with pytest.raises(ValueError): # The correlation value of two same columns is always maximal
                gain_ratio(idadf, target= idadf.columns[0], features = idadf.columns[0], symmetry=True)
                
    def test_sym_gain_ratio_one_target(self, idadf):
        if len(idadf.columns) > 1:
            result = gain_ratio(idadf, target = idadf.columns[0], symmetry=True)
            assert(isinstance(result, pandas.core.series.Series))
            assert(len(result) == (len(idadf.columns)-1))
            
    def test_sym_gain_ratio_multiple_target(self, idadf):
        if len(idadf.columns) > 1:
            result = gain_ratio(idadf, target = [idadf.columns[0],idadf.columns[1]], symmetry=True)
            assert(isinstance(result, pandas.core.frame.DataFrame))
            assert(len(result.columns) == 2)
            assert(len(result.index) == len(idadf.columns))
            
    def test_sym_gain_ratio_one_target_one_feature(self, idadf):
        if len(idadf.columns) > 1:
            result = gain_ratio(idadf, target = idadf.columns[0], features=[idadf.columns[1]], symmetry=True)
            assert(isinstance(result, float))
            result2 = gain_ratio(idadf, target = idadf.columns[1], features=[idadf.columns[0]], symmetry=True)
            assert(round(result,3) == round(result2,3)) # symmetry
    
class Test_Asymmetric_Gain_Ratio(object):

    def test_asym_gain_ratio_default(self, idadf):
        if len(idadf.columns) > 1:
            result = gain_ratio(idadf, features = idadf.columns, symmetry=False)
            assert(isinstance(result, pandas.core.frame.DataFrame))
            assert(len(result.columns) == len(idadf.columns))
            assert(len(result.index) == len(idadf.columns))
            result2 = gain_ratio(idadf, symmetry=False)
            assert(all(result == result2))
            result = result.fillna(0) # np.nan values are not equal when compared
            assert(any(result != result.T)) # asymmetry
            
    def test_asym_gain_ratio_valueError(self, idadf):
        if len(idadf.columns) > 0: 
            with pytest.raises(ValueError): # Cannot compute correlation coefficients of only one column (...), need at least 2
                gain_ratio(idadf, features = idadf.columns[0], symmetry=False)
            with pytest.raises(ValueError): # The correlation value of two same columns is always maximal
                gain_ratio(idadf, target= idadf.columns[0], features = idadf.columns[0], symmetry=False)
                
    def test_asym_gain_ratio_one_target(self, idadf):
        if len(idadf.columns) > 1:
            result = gain_ratio(idadf, target = idadf.columns[0], symmetry=False)
            assert(isinstance(result, pandas.core.series.Series))
            assert(len(result) == (len(idadf.columns)-1))
            
    def test_asym_gain_ratio_multiple_target(self, idadf):
        if len(idadf.columns) > 1:
            result = gain_ratio(idadf, target = [idadf.columns[0],idadf.columns[1]], symmetry=False)
            assert(isinstance(result, pandas.core.frame.DataFrame))
            assert(len(result.columns) == 2)
            assert(len(result.index) == len(idadf.columns))
            
    def test_asym_gain_ratio_one_target_one_feature(self, idadf):
        if len(idadf.columns) > 1:
            result = gain_ratio(idadf, target = idadf.columns[0], features=[idadf.columns[1]], symmetry=False)
            assert(isinstance(result, float))
            
class Test_Symmetric_Uncertainty(object):

    def test_su_default(self, idadf):
        if len(idadf.columns) > 1:
            result = su(idadf, features = idadf.columns)
            assert(isinstance(result, pandas.core.frame.DataFrame))
            assert(len(result.columns) == len(idadf.columns))
            assert(len(result.index) == len(idadf.columns))
            result2 = su(idadf)
            assert(all(result == result2))
            result = result.fillna(0) # np.nan values are not equal when compared
            assert(all(result == result.T)) # symmetry
            
    def test_su_valueError(self, idadf):
        if len(idadf.columns) > 0: 
            with pytest.raises(ValueError): # Cannot compute correlation coefficients of only one column (...), need at least 2
                su(idadf, features = idadf.columns[0])
            with pytest.raises(ValueError): # The correlation value of two same columns is always maximal
                su(idadf, target= idadf.columns[0], features = idadf.columns[0])
                
    def test_su_one_target(self, idadf):
        if len(idadf.columns) > 1:
            result = su(idadf, target = idadf.columns[0])
            assert(isinstance(result, pandas.core.series.Series))
            assert(len(result) == (len(idadf.columns)-1))
            
    def test_su_multiple_target(self, idadf):
        if len(idadf.columns) > 1:
            result = su(idadf, target = [idadf.columns[0],idadf.columns[1]])
            assert(isinstance(result, pandas.core.frame.DataFrame))
            assert(len(result.columns) == 2)
            assert(len(result.index) == len(idadf.columns))
            
    def test_su_one_target_one_feature(self, idadf):
        if len(idadf.columns) > 1:
            result = su(idadf, target = idadf.columns[0], features=[idadf.columns[1]])
            assert(isinstance(result, float))
            result2 = su(idadf, target = idadf.columns[1], features=[idadf.columns[0]])
            assert(round(result,3) == round(result2,3)) # symmetry