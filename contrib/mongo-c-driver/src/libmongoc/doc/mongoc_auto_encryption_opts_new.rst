:man_page: mongoc_auto_encryption_opts_new

mongoc_auto_encryption_opts_new()
=================================

Synopsis
--------

.. code-block:: c

  mongoc_auto_encryption_opts_t *
  mongoc_auto_encryption_opts_new (void) BSON_GNUC_WARN_UNUSED_RESULT;


Create a new :symbol:`mongoc_auto_encryption_opts_t`.

Caller must set the required options:

* :symbol:`mongoc_auto_encryption_opts_set_keyvault_namespace()`
* :symbol:`mongoc_auto_encryption_opts_set_kms_providers()`

Caller may set optionally set the following:

* :symbol:`mongoc_auto_encryption_opts_set_keyvault_client()`
* :symbol:`mongoc_auto_encryption_opts_set_schema_map()`
* :symbol:`mongoc_auto_encryption_opts_set_bypass_auto_encryption()`
* :symbol:`mongoc_auto_encryption_opts_set_extra()`

This options struct is used to enable auto encryption with :symbol:`mongoc_client_enable_auto_encryption()`.

Returns
-------

A new :symbol:`mongoc_auto_encryption_opts_t`, which must be destroyed with :symbol:`mongoc_auto_encryption_opts_destroy()`.

.. seealso::

  | :symbol:`mongoc_auto_encryption_opts_destroy()`

  | :symbol:`mongoc_client_enable_auto_encryption()`

  | `In-Use Encryption <in-use-encryption_>`_

