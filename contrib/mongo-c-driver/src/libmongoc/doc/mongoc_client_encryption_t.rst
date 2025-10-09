:man_page: mongoc_client_encryption_t

mongoc_client_encryption_t
==========================

Synopsis
--------

.. code-block:: c

   typedef struct _mongoc_client_encryption_t mongoc_client_encryption_t;


``mongoc_client_encryption_t`` provides utility functions for `In-Use Encryption <in-use-encryption_>`_.

Thread Safety
-------------

:symbol:`mongoc_client_encryption_t` is NOT thread-safe and should only be used in the same thread as the :symbol:`mongoc_client_t` that is configured via :symbol:`mongoc_client_encryption_opts_set_keyvault_client()`.

Lifecycle
---------

The key vault client, configured via :symbol:`mongoc_client_encryption_opts_set_keyvault_client()`, must outlive the :symbol:`mongoc_client_encryption_t`.

.. only:: html

  Functions
  ---------

  .. toctree::
    :titlesonly:
    :maxdepth: 1

    mongoc_client_encryption_new
    mongoc_client_encryption_destroy
    mongoc_client_encryption_create_datakey
    mongoc_client_encryption_create_encrypted_collection
    mongoc_client_encryption_rewrap_many_datakey
    mongoc_client_encryption_delete_key
    mongoc_client_encryption_get_crypt_shared_version
    mongoc_client_encryption_get_key
    mongoc_client_encryption_get_keys
    mongoc_client_encryption_add_key_alt_name
    mongoc_client_encryption_remove_key_alt_name
    mongoc_client_encryption_get_key_by_alt_name
    mongoc_client_encryption_encrypt
    mongoc_client_encryption_encrypt_expression
    mongoc_client_encryption_decrypt

.. seealso::

  | :symbol:`mongoc_client_enable_auto_encryption()`

  | :symbol:`mongoc_client_pool_enable_auto_encryption()`

  | `In-Use Encryption <in-use-encryption_>`_ for libmongoc

  | The MongoDB Manual for `Client-Side Field Level Encryption <https://www.mongodb.com/docs/manual/core/security-client-side-encryption/>`_

  | The MongoDB Manual for `Queryable Encryption <https://www.mongodb.com/docs/manual/core/queryable-encryption/>`_
