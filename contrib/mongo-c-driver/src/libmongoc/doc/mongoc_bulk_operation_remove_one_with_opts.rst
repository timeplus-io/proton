:man_page: mongoc_bulk_operation_remove_one_with_opts

mongoc_bulk_operation_remove_one_with_opts()
============================================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_bulk_operation_remove_one_with_opts (mongoc_bulk_operation_t *bulk,
                                              const bson_t *selector,
                                              const bson_t *opts,
                                              bson_error_t *error); /* OUT */

Remove a single document as part of a bulk operation. This only queues the operation. To execute it, call :symbol:`mongoc_bulk_operation_execute()`.

Parameters
----------

* ``bulk``: A :symbol:`mongoc_bulk_operation_t`.
* ``selector``: A :symbol:`bson:bson_t` that selects which document to remove.
* ``error``: An optional location for a :symbol:`bson_error_t <errors>` or ``NULL``.

.. include:: includes/bulk-remove-one-opts.txt

Errors
------

Operation errors are propagated via :symbol:`mongoc_bulk_operation_execute()`, while argument validation errors are reported by the ``error`` argument.

Returns
-------

Returns true on success, and false if passed invalid arguments.

.. seealso::

  | :symbol:`mongoc_bulk_operation_remove_one()`

  | :symbol:`mongoc_bulk_operation_remove_many_with_opts()`

