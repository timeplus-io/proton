:man_page: mongoc_client_encryption_opts_new

mongoc_client_encryption_opts_new()
===================================

Synopsis
--------

.. code-block:: c

  mongoc_client_encryption_opts_t *
  mongoc_client_encryption_opts_new (void) BSON_GNUC_WARN_UNUSED_RESULT;

Returns
-------

A new :symbol:`mongoc_client_encryption_opts_t` that must be freed with :symbol:`mongoc_client_encryption_opts_destroy()`.