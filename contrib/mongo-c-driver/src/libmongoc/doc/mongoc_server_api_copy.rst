:man_page: mongoc_server_api_copy

mongoc_server_api_copy()
========================

Synopsis
--------

.. code-block:: c

  mongoc_server_api_t *
  mongoc_server_api_copy (const mongoc_server_api_t *api)
     BSON_GNUC_WARN_UNUSED_RESULT;

Creates a deep copy of ``api``.

Parameters
----------

* ``api``: A :symbol:`mongoc_server_api_t`.

Returns
-------

Returns a newly allocated copy of ``api`` that must be freed with :symbol:`mongoc_server_api_destroy()` when no longer in use.
