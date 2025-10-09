:man_page: mongoc_client_find_databases_with_opts

mongoc_client_find_databases_with_opts()
========================================

Synopsis
--------

.. code-block:: c

  mongoc_cursor_t *
  mongoc_client_find_databases_with_opts (
     mongoc_client_t *client, const bson_t *opts) BSON_GNUC_WARN_UNUSED_RESULT;

Fetches a cursor containing documents, each corresponding to a database on this MongoDB server.

.. include:: includes/retryable-read.txt

Parameters
----------

* ``client``: A :symbol:`mongoc_client_t`.
* ``opts``: A :symbol:`bson:bson_t` containing additional options.

.. |opts-source| replace:: ``client``

.. include:: includes/generic-opts.txt

For a list of all options, see `the MongoDB Manual entry on the listDatabases command <https://www.mongodb.com/docs/manual/reference/command/listDatabases/>`_.

Errors
------

Use :symbol:`mongoc_cursor_error` on the returned cursor to check for errors.

Returns
-------

A cursor where each result corresponds to the server's representation of a database.

The cursor functions :symbol:`mongoc_cursor_set_limit`, :symbol:`mongoc_cursor_set_batch_size`, and :symbol:`mongoc_cursor_set_max_await_time_ms` have no use on the returned cursor.
