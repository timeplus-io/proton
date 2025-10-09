:man_page: mongoc_server_api_new

mongoc_server_api_new()
=======================

Synopsis
--------

.. code-block:: c

  mongoc_server_api_t *
  mongoc_server_api_new (mongoc_server_api_version_t version)
     BSON_GNUC_WARN_UNUSED_RESULT;

Create a struct to hold server API preferences.

Returns
-------

A new ``mongoc_server_api_t`` you must free with :symbol:`mongoc_server_api_destroy`.
