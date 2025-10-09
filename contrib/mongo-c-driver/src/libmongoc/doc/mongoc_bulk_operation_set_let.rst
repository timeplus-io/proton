:man_page: mongoc_bulk_operation_set_let

mongoc_bulk_operation_set_let()
===============================

Synopsis
--------

.. code-block:: c

  void
  mongoc_bulk_operation_set_let (
     mongoc_bulk_operation_t *bulk, const bson_t *let);

Parameters
----------

* ``bulk``: A :symbol:`mongoc_bulk_operation_t`.
* ``let``: A BSON document consisting of any number of parameter names, each followed by definitions of constants in the MQL Aggregate Expression language.

Description
-----------

Defines constants that can be accessed by all update, replace, and delete operations executed as part of this :doc:`bulk <mongoc_bulk_operation_t>`.

It is prohibited to call this function after adding operations to the :doc:`bulk <mongoc_bulk_operation_t>`.
