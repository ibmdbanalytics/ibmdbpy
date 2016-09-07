.. highlight:: python

Shared functions and methods
****************************

Custom Exceptions
=================

.. currentmodule:: ibmdbpy.exceptions

Error
-----
.. autoclass:: Error

IdaDataBaseError
----------------
.. autoclass:: IdaDataBaseError

IdaDataFrameError
-----------------
.. autoclass:: IdaDataFrameError

PrimaryKeyError
---------------
.. autoclass:: PrimaryKeyError

IdaKMeansError
--------------
.. autoclass:: IdaKMeansError

IdaAssociationRulesError
------------------------
.. autoclass:: IdaAssociationRulesError

IdaNaiveBayesError
------------------
.. autoclass:: IdaNaiveBayesError

User Interactions
=================

.. currentmodule:: ibmdbpy.utils

Configuring the environment
---------------------------

.. autofunction:: set_verbose

.. autofunction:: set_autocommit

Performance Monitoring
----------------------
.. autofunction:: timed

User prompt
-----------
.. autofunction:: query_yes_no

Legagy from benchmark
---------------------

.. autofunction:: chunklist

.. autofunction:: silent

.. autofunction:: to_nK

.. autofunction:: extend_dataset

Convention checking
===================

Name checking
-------------
.. autofunction:: check_tablename
.. autofunction:: check_viewname
.. autofunction:: check_case

Private functions
=================

_convert_dtypes
---------------
.. autofunction:: _convert_dtypes

_reset_attributes
-----------------
.. autofunction:: _reset_attributes