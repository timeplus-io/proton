:man_page: mongoc_collection_find_indexes

mongoc_collection_find_indexes()
================================

.. warning::
   .. deprecated:: 1.9.0

      This function is deprecated and should not be used in new code.

      Please use :symbol:`mongoc_collection_find_indexes_with_opts()` in new code.

Synopsis
--------

.. code-block:: c

  mongoc_cursor_t *
  mongoc_collection_find_indexes (mongoc_collection_t *collection,
                                  bson_error_t *error);
     BSON_GNUC_WARN_UNUSED_RESULT
     BSON_GNUC_DEPRECATED_FOR (mongoc_collection_find_indexes_with_opts);

Fetches a cursor containing documents, each corresponding to an index on this collection.

.. include:: includes/retryable-read.txt

Parameters
----------

* ``collection``: A :symbol:`mongoc_collection_t`.
* ``error``: An optional location for a :symbol:`bson_error_t <errors>` or ``NULL``.

Errors
------

Errors are propagated via the ``error`` parameter.

Returns
-------

.. include:: includes/returns-cursor.txt

In the returned cursor each result corresponds to the server's representation of an index on this collection. If the collection does not exist on the server, the cursor will be empty.

The cursor functions :symbol:`mongoc_cursor_set_limit`, :symbol:`mongoc_cursor_set_batch_size`, and :symbol:`mongoc_cursor_set_max_await_time_ms` have no use on the returned cursor.

