:man_page: mongoc_collection_create_bulk_operation_with_opts

mongoc_collection_create_bulk_operation_with_opts()
===================================================

Synopsis
--------

.. code-block:: c

  mongoc_bulk_operation_t *
  mongoc_collection_create_bulk_operation_with_opts (
     mongoc_collection_t *collection,
     const bson_t *opts) BSON_GNUC_WARN_UNUSED_RESULT;

Parameters
----------

* ``collection``: A :symbol:`mongoc_collection_t`.

.. |opts-source| replace:: ``collection``

.. include:: includes/bulk-opts.txt

Description
-----------

This function shall begin a new bulk operation. After creating this you may call various functions such as :symbol:`mongoc_bulk_operation_update()`, :symbol:`mongoc_bulk_operation_insert()` and others.

After calling :symbol:`mongoc_bulk_operation_execute()` the commands will be executed in as large as batches as reasonable by the client.

Errors
------

Errors are propagated when executing the bulk operation.

Returns
-------

A newly allocated :symbol:`mongoc_bulk_operation_t` that should be freed with :symbol:`mongoc_bulk_operation_destroy()` when no longer in use.

.. warning::

  Failure to handle the result of this function is a programming error.

.. seealso::

  | `Bulk Write Operations <bulk_>`_

  | :symbol:`mongoc_bulk_operation_t`

