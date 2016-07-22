.. highlight:: python

IdaDataFrame
************

Open an IdaDataFrame Object.
============================

.. currentmodule:: ibmdbpy.frame

.. autoclass:: IdaDataFrame

   .. automethod:: __init__

.. rubric:: Methods


DataFrame introspection
=======================

internal_state
--------------
.. autoattribute:: IdaDataFrame.internal_state

indexer
-------
.. autoattribute:: IdaDataFrame.indexer

type
----
.. autoattribute:: IdaDataFrame.type

dtypes
------
.. autoattribute:: IdaDataFrame.dtypes

index
-----
.. autoattribute:: IdaDataFrame.index

columns
-------
.. autoattribute:: IdaDataFrame.columns

axes
----
.. autoattribute:: IdaDataFrame.axes

shape
-----
.. autoattribute:: IdaDataFrame.shape

empty
-----
.. autoattribute:: IdaDataFrame.empty

__len__
-------
.. automethod:: IdaDataFrame.__len__

__iter__
---------
.. automethod:: IdaDataFrame.__iter__

DataFrame modification
======================

Selection, Projection
---------------------
.. automethod:: IdaDataFrame.__getitem__

Selection and Projection are also possible using the ``ibmdbpy.Loc`` object stored in ``IdaDataFrame.loc``.

.. autoclass:: ibmdbpy.indexing.Loc
	:members:


Filtering
---------

.. autoclass:: ibmdbpy.filtering.FilterQuery
	:members:

.. automethod:: IdaDataFrame.__lt__

.. automethod:: IdaDataFrame.__le__

.. automethod:: IdaDataFrame.__eq__

.. automethod:: IdaDataFrame.__ne__

.. automethod:: IdaDataFrame.__ge__

.. automethod:: IdaDataFrame.__gt__


Feature Engineering
-------------------

.. automethod:: IdaDataFrame.__setitem__

.. automethod:: ibmdbpy.aggregation.aggregate_idadf

.. currentmodule:: ibmdbpy.frame

.. automethod:: IdaDataFrame.__add__

.. automethod:: IdaDataFrame.__radd__

.. automethod:: IdaDataFrame.__div__

.. automethod:: IdaDataFrame.__rdiv__

.. automethod:: IdaDataFrame.__truediv__

.. automethod:: IdaDataFrame.__rtruediv__

.. automethod:: IdaDataFrame.__floordiv__

.. automethod:: IdaDataFrame.__rfloordiv__

.. automethod:: IdaDataFrame.__mod__

.. automethod:: IdaDataFrame.__rmod__

.. automethod:: IdaDataFrame.__mul__

.. automethod:: IdaDataFrame.__rmul__

.. automethod:: IdaDataFrame.__neg__

.. automethod:: IdaDataFrame.__rpos__

.. automethod:: IdaDataFrame.__pow__

.. automethod:: IdaDataFrame.__rpow__

.. automethod:: IdaDataFrame.__sub__

.. automethod:: IdaDataFrame.__rsub__

.. automethod:: IdaDataFrame.__delitem__

.. automethod:: IdaDataFrame.save_as

DataBase Features
=================

exists
------
.. automethod:: IdaDataFrame.exists

is_view / is_table
------------------
.. automethod:: IdaDataFrame.is_view

.. automethod:: IdaDataFrame.is_table


get_primary_key
---------------
.. automethod:: IdaDataFrame.get_primary_key

ida_query
---------
.. automethod:: IdaDataFrame.ida_query

ida_scalar_query
----------------
.. automethod:: IdaDataFrame.ida_scalar_query


Data Exploration
================

head
----
.. automethod:: IdaDataFrame.head

tail
----
.. automethod:: IdaDataFrame.tail

pivot_table
-----------
.. automethod:: IdaDataFrame.pivot_table

sort
----
.. automethod:: IdaDataFrame.sort


Descriptive Statistics
======================

describe
--------
.. automethod:: IdaDataFrame.describe

cov (covariance)
----------------
.. automethod:: IdaDataFrame.cov

corr (correlation)
------------------
.. automethod:: IdaDataFrame.corr

quantile
--------
.. automethod:: IdaDataFrame.quantile

mad (mean absolute deviation)
-----------------------------
.. automethod:: IdaDataFrame.mad

min (minimum)
-------------
.. automethod:: IdaDataFrame.min

max (maximum)
-------------
.. automethod:: IdaDataFrame.max

count
-----
.. automethod:: IdaDataFrame.count

count_distinct
--------------
.. automethod:: IdaDataFrame.count_distinct

std (standard deviation)
------------------------
.. automethod:: IdaDataFrame.std

var (variance)
--------------
.. automethod:: IdaDataFrame.var

mean
----
.. automethod:: IdaDataFrame.mean

sum
---
.. automethod:: IdaDataFrame.sum

median
------
.. automethod:: IdaDataFrame.median

Import as DataFrame
===================

as_dataframe
------------
.. automethod:: IdaDataFrame.as_dataframe

Connection Management
=====================

commit
------
.. automethod:: IdaDataFrame.commit

rollback
--------
.. automethod:: IdaDataFrame.rollback


Private Methods
===============

_clone
------
.. automethod:: IdaDataFrame._clone

_clone_as_serie
---------------
.. automethod:: IdaDataFrame._clone_as_serie

_get_type
---------
.. automethod:: IdaDataFrame._get_type

_get_columns
------------
.. automethod:: IdaDataFrame._get_columns

_get_all_columns_in_table
-------------------------
.. automethod:: IdaDataFrame._get_all_columns_in_table

_get_index
----------
.. automethod:: IdaDataFrame._get_index

_get_shape
----------
.. automethod:: IdaDataFrame._get_shape

_get_columns_dtypes
-------------------
.. automethod:: IdaDataFrame._get_columns_dtypes

_reset_attributes
-----------------
.. automethod:: IdaDataFrame._reset_attributes

_table_def
----------
.. automethod:: IdaDataFrame._table_def

_get_numerical_columns
----------------------
.. automethod:: IdaDataFrame._get_numerical_columns

_get_categorical_columns
------------------------
.. automethod:: IdaDataFrame._get_categorical_columns

_prepare_and_execute
--------------------
.. automethod:: IdaDataFrame._prepare_and_execute

_autocommit
-----------
.. automethod:: IdaDataFrame._autocommit

_combine_check
--------------
.. automethod:: IdaDataFrame._combine_check

These functions are defined in ibmdbpy.statistics but apply to IdaDataFrames.




_numeric_stats
--------------
.. autofunction:: ibmdbpy.statistics._numeric_stats

_get_percentiles
----------------
.. autofunction:: ibmdbpy.statistics._get_percentiles

_categorical_stats
------------------
.. autofunction:: ibmdbpy.statistics._categorical_stats

_get_number_of_nas
------------------
.. autofunction:: ibmdbpy.statistics._get_number_of_nas

_count_level
------------
.. autofunction:: ibmdbpy.statistics._count_level

_count_level_groupby
--------------------
.. autofunction:: ibmdbpy.statistics._count_level_groupby

_factors_count
--------------------
.. autofunction:: ibmdbpy.statistics._factors_count

_factors_sum
--------------------
.. autofunction:: ibmdbpy.statistics._factors_sum

_factors_avg
--------------------
.. autofunction:: ibmdbpy.statistics._factors_avg