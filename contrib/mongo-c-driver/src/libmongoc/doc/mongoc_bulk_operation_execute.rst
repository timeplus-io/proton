:man_page: mongoc_bulk_operation_execute

mongoc_bulk_operation_execute()
===============================

Synopsis
--------

.. code-block:: c

  uint32_t
  mongoc_bulk_operation_execute (mongoc_bulk_operation_t *bulk,
                                 bson_t *reply,
                                 bson_error_t *error);

This function executes all operations queued into the bulk operation. Unless ``ordered: false`` was specified in the ``opts`` passed to :symbol:`mongoc_collection_create_bulk_operation_with_opts()`, then forward progress will be stopped upon the first error.

It is only valid to call :symbol:`mongoc_bulk_operation_execute()` once. The ``mongoc_bulk_operation_t`` must be destroyed afterwards.

.. warning::

  ``reply`` is always initialized, even upon failure. Callers *must* call :symbol:`bson:bson_destroy()` to release this potential allocation.

Parameters
----------

* ``bulk``: A :symbol:`mongoc_bulk_operation_t`.
* ``reply``: A |bson_t-opt-storage-ptr| to contain the results.
* ``error``: An optional location for a :symbol:`bson_error_t <errors>` or ``NULL``.

Errors
------

Errors are propagated via the ``error`` parameter.

Returns
-------

On success, returns the server id used. On failure, returns 0 and sets ``error``.

A write concern timeout or write concern error is considered a failure.

The ``reply`` document counts operations and collects error information. See `Bulk Write Operations <bulk_>`_ for examples.

.. seealso::

  | `Bulk Write Operations <bulk_>`_

  | :symbol:`mongoc_bulk_operation_get_hint`, which gets the id of the server used even if the operation failed.

