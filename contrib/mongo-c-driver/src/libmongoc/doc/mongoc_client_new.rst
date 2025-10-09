:man_page: mongoc_client_new

mongoc_client_new()
===================

Synopsis
--------

.. code-block:: c

  mongoc_client_t *
  mongoc_client_new (const char *uri_string) BSON_GNUC_WARN_UNUSED_RESULT;

Creates a new :symbol:`mongoc_client_t` using the URI string provided.

Parameters
----------

* ``uri_string``: A string containing the MongoDB connection URI.

Returns
-------

A newly allocated :symbol:`mongoc_client_t` that should be freed with :symbol:`mongoc_client_destroy()` when no longer in use. On error, ``NULL`` is returned and an error or warning will be logged.

.. seealso::

  | :symbol:`mongoc_client_new_from_uri()`

