:man_page: mongoc_cursor_set_max_await_time_ms

mongoc_cursor_set_max_await_time_ms()
=====================================

Synopsis
--------

.. code-block:: c

  void
  mongoc_cursor_set_max_await_time_ms (mongoc_cursor_t *cursor,
                                       uint32_t max_await_time_ms);

Parameters
----------

* ``cursor``: A :symbol:`mongoc_cursor_t`.
* ``max_await_time_ms``: A timeout in milliseconds.

Description
-----------

The maximum amount of time for the server to wait on new documents to satisfy a tailable cursor query. Only applies if the cursor is created from :symbol:`mongoc_collection_find_with_opts` with "tailable" and "awaitData" options, and the server is MongoDB 3.2 or later. See `the documentation for maxTimeMS and the "getMore" command <https://www.mongodb.com/docs/master/reference/command/getMore/>`_.

The ``max_await_time_ms`` cannot be changed after the first call to :symbol:`mongoc_cursor_next`.

This is not applicable to all cursors. Calling :symbol:`mongoc_cursor_set_batch_size` on a cursor returned by :symbol:`mongoc_client_find_databases_with_opts`, :symbol:`mongoc_database_find_collections_with_opts`, or :symbol:`mongoc_collection_find_indexes_with_opts` will not change the results.

Note: although ``max_await_time_ms`` is a uint32_t, it is possible to set it as a uint64_t through the options arguments in some cursor returning functions like :symbol:`mongoc_collection_find_with_opts()`.

.. seealso::

  | `Tailable Cursors. <cursors_tailable_>`_

