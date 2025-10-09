:man_page: mongoc_cursor_set_batch_size

mongoc_cursor_set_batch_size()
==============================

Synopsis
--------

.. code-block:: c

  void
  mongoc_cursor_set_batch_size (mongoc_cursor_t *cursor, uint32_t batch_size);

Parameters
----------

* ``cursor``: A :symbol:`mongoc_cursor_t`.
* ``batch_size``: The requested number of documents per batch.

Description
-----------

Limits the number of documents returned in one batch. Each batch requires a round trip to the server. If the batch size is zero, the cursor uses the server-defined maximum batch size.

See `Cursor Batches <https://www.mongodb.com/docs/manual/core/cursors/#cursor-batches>`_ in the MongoDB Manual.

This is not applicable to all cursors. Calling :symbol:`mongoc_cursor_set_batch_size` on a cursor returned by :symbol:`mongoc_client_find_databases_with_opts`, :symbol:`mongoc_database_find_collections_with_opts`, or :symbol:`mongoc_collection_find_indexes_with_opts` will not change the results.