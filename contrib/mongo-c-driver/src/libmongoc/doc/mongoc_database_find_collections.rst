:man_page: mongoc_database_find_collections

mongoc_database_find_collections()
==================================

.. warning::
   .. deprecated:: 1.9.0

      This function is deprecated and should not be used in new code.

      Please use :symbol:`mongoc_database_find_collections_with_opts()` in new code.

Synopsis
--------

.. code-block:: c

  mongoc_cursor_t *
  mongoc_database_find_collections (mongoc_database_t *database,
                                    const bson_t *filter,
                                    bson_error_t *error)
     BSON_GNUC_WARN_UNUSED_RESULT
     BSON_GNUC_DEPRECATED_FOR (mongoc_database_find_collections_with_opts);

Description
-----------

Fetches a cursor containing documents, each corresponding to a collection on this database.

.. include:: includes/retryable-read.txt

Parameters
----------

* ``database``: A :symbol:`mongoc_database_t`.
* ``filter``: A matcher used by the server to filter the returned collections. May be ``NULL``.
* ``error``: An optional location for a :symbol:`bson_error_t <errors>` or ``NULL``.

Errors
------

Errors are propagated via the ``error`` parameter.

Returns
-------

This function returns a newly allocated :symbol:`mongoc_cursor_t` that should be freed with :symbol:`mongoc_cursor_destroy()` when no longer in use, or `NULL` in case of error. The user must call :symbol:`mongoc_cursor_next()` on the returned :symbol:`mongoc_cursor_t` to execute the initial command.

In the returned cursor each result corresponds to the server's representation of a collection in this database.

The cursor functions :symbol:`mongoc_cursor_set_limit`, :symbol:`mongoc_cursor_set_batch_size`, and :symbol:`mongoc_cursor_set_max_await_time_ms` have no use on the returned cursor.
