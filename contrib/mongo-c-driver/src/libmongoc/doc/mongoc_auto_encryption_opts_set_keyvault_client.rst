:man_page: mongoc_auto_encryption_opts_set_key_vault_client

mongoc_auto_encryption_opts_set_keyvault_client()
=================================================

Synopsis
--------

.. code-block:: c

   void
   mongoc_auto_encryption_opts_set_keyvault_client (
      mongoc_auto_encryption_opts_t *opts, mongoc_client_t *client);

Set an optional separate :symbol:`mongoc_client_t` to use during key lookup for automatic encryption and decryption. Only applies to automatic encryption on a single-threaded :symbol:`mongoc_client_t`.

Parameters
----------

* ``opts``: A :symbol:`mongoc_auto_encryption_opts_t`.
* ``client``: A :symbol:`mongoc_client_t` to use for key queries. This client should *not* have automatic encryption enabled, as it will only execute ``find`` commands against the key vault collection to retrieve keys for automatic encryption and decryption. This ``client`` MUST outlive any :symbol:`mongoc_client_t` which has been enabled to use it through :symbol:`mongoc_client_enable_auto_encryption()`.

.. seealso::

  | :symbol:`mongoc_client_enable_auto_encryption()`

  | :symbol:`mongoc_auto_encryption_opts_set_keyvault_client_pool()`

  | `In-Use Encryption <in-use-encryption_>`_

