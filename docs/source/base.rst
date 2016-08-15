.. highlight:: python

IdaDataBase
***********

Connect to dashDB/DB2
=====================

.. currentmodule:: ibmdbpy.base

.. autoclass:: IdaDataBase

   .. automethod:: __init__

.. rubric:: Methods

DataBase Exploration
====================

current_schema
--------------
.. automethod:: IdaDataBase.current_schema

show_tables
-----------
.. automethod:: IdaDataBase.show_tables

show_models
-----------
.. automethod:: IdaDataBase.show_models

exists_table_or_view
--------------------
.. automethod:: IdaDataBase.exists_table_or_view

exists_table
------------
.. automethod:: IdaDataBase.exists_table

exists_view
-----------
.. automethod:: IdaDataBase.exists_view

exists_model
------------
.. automethod:: IdaDataBase.exists_model

is_table_or_view
----------------
.. automethod:: IdaDataBase.is_table_or_view

is_table
--------
.. automethod:: IdaDataBase.is_table

is_view
-------
.. automethod:: IdaDataBase.is_view

is_model
--------
.. automethod:: IdaDataBase.is_model

ida_query
---------
.. automethod:: IdaDataBase.ida_query

ida_scalar_query
----------------
.. automethod:: IdaDataBase.ida_scalar_query

Upload DataFrames
=================

as_idadataframe
---------------
.. automethod:: IdaDataBase.as_idadataframe

Delete DataBase Objects
=======================

drop_table
----------
.. automethod:: IdaDataBase.drop_table

drop_view
---------
.. automethod:: IdaDataBase.drop_view

drop_model
----------
.. automethod:: IdaDataBase.drop_model

DataBase Modification
=====================

rename
------
.. automethod:: IdaDataBase.rename

add_column_id
-------------
.. automethod:: IdaDataBase.add_column_id

delete_column
-------------
.. automethod:: IdaDataBase.delete_column

append
------
.. automethod:: IdaDataBase.append


Connection Management
=====================

commit
------
.. automethod:: IdaDataBase.commit

rollback
--------
.. automethod:: IdaDataBase.rollback

close
-----
.. automethod:: IdaDataBase.close

reconnect
---------
.. automethod:: IdaDataBase.reconnect

Private Methods
===============

_exists
-------
.. automethod:: IdaDataBase._exists

_is
---
.. automethod:: IdaDataBase._is

_drop
-----
.. automethod:: IdaDataBase._drop

_upper_columns
--------------
.. automethod:: IdaDataBase._upper_columns

_get_name_and_schema
--------------------
.. automethod:: IdaDataBase._get_name_and_schema

_get_valid_tablename
--------------------
.. automethod:: IdaDataBase._get_valid_tablename

_get_valid_viewname
--------------------
.. automethod:: IdaDataBase._get_valid_viewname

_get_valid_modelname
--------------------
.. automethod:: IdaDataBase._get_valid_modelname

_create_table
-------------
.. automethod:: IdaDataBase._create_table

_create_view
------------
.. automethod:: IdaDataBase._create_view

_insert_into_database
---------------------
.. automethod:: IdaDataBase._insert_into_database

_prepare_and_execute
--------------------
.. automethod:: IdaDataBase._prepare_and_execute

_check_procedure
----------------
.. automethod:: IdaDataBase._check_procedure

_call_stored_procedure
----------------------
.. automethod:: IdaDataBase._call_stored_procedure

_autocommit
-----------
.. automethod:: IdaDataBase._autocommit

_check_connection
-----------------
.. automethod:: IdaDataBase._check_connection

_retrieve_cache
---------------
.. automethod:: IdaDataBase._retrieve_cache

_reset_attributes
-----------------
.. automethod:: IdaDataBase._reset_attributes