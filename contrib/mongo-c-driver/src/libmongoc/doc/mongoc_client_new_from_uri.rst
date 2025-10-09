:man_page: mongoc_client_new_from_uri

mongoc_client_new_from_uri()
============================

Synopsis
--------

.. code-block:: c

  mongoc_client_t *
  mongoc_client_new_from_uri (const mongoc_uri_t *uri)
     BSON_GNUC_WARN_UNUSED_RESULT;

Creates a new :symbol:`mongoc_client_t` using the :symbol:`mongoc_uri_t` provided.

Parameters
----------

* ``uri``: A :symbol:`mongoc_uri_t`.

Returns
-------

A newly allocated :symbol:`mongoc_client_t` that should be freed with :symbol:`mongoc_client_destroy()` when no longer in use. On error, ``NULL`` is returned and an error will be logged.

.. seealso::

  | :symbol:`mongoc_client_new_from_uri_with_error()`

