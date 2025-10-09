:man_page: mongoc_client_new_from_uri_with_error

mongoc_client_new_from_uri_with_error()
=======================================

Synopsis
--------

.. code-block:: c

  mongoc_client_t *
  mongoc_client_new_from_uri_with_error (const mongoc_uri_t *uri,
                                         bson_error_t *error)
     BSON_GNUC_WARN_UNUSED_RESULT;

Creates a new :symbol:`mongoc_client_t` using the :symbol:`mongoc_uri_t` provided.

Parameters
----------

* ``uri``: A :symbol:`mongoc_uri_t`.
* ``error``: An optional location for a :symbol:`bson_error_t <errors>` or ``NULL``.

Returns
-------

A newly allocated :symbol:`mongoc_client_t` that should be freed with :symbol:`mongoc_client_destroy()` when no longer in use. On error, ``NULL`` is returned and ``error`` will be populated with the error description.

