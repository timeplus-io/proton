:man_page: mongoc_client_encryption_new

mongoc_client_encryption_new()
==============================

Synopsis
--------

.. code-block:: c

  mongoc_client_encryption_t *
  mongoc_client_encryption_new (mongoc_client_encryption_opts_t *opts,
                                bson_error_t *error) BSON_GNUC_WARN_UNUSED_RESULT;

Create a new :symbol:`mongoc_client_encryption_t`.

Parameters
----------

* ``opts``: A :symbol:`mongoc_client_encryption_opts_t`.
* ``error``: A :symbol:`bson_error_t`.

Returns
-------

A new :symbol:`mongoc_client_encryption_t` that must be freed with :symbol:`mongoc_client_encryption_destroy()` if successful. Returns ``NULL`` and sets ``error`` otherwise.

.. seealso::

  | :symbol:`mongoc_client_encryption_t`

  | :symbol:`mongoc_client_encryption_opts_t`

