:man_page: mongoc_client_encryption_opts_set_keyvault_client

mongoc_client_encryption_opts_set_keyvault_client()
===================================================

Synopsis
--------

.. code-block:: c

   void
   mongoc_client_encryption_opts_set_keyvault_client (
      mongoc_client_encryption_opts_t *opts,
      mongoc_client_t *keyvault_client);

Set the :symbol:`mongoc_client_t` to use during key creation and key lookup for encryption and decryption. This may be either a single-threaded or multi-threaded client (i.e. a client obtained from a :symbol:`mongoc_client_pool_t`).

Parameters
----------

* ``opts``: A :symbol:`mongoc_client_encryption_opts_t`.
* ``client``: A :symbol:`mongoc_client_t` to use for key lookup and creation. This ``client`` MUST outlive any :symbol:`mongoc_client_encryption_t` configured to use it with :symbol:`mongoc_client_encryption_new()`.

.. seealso::

  | :symbol:`mongoc_client_encryption_new()`

  | `In-Use Encryption <in-use-encryption_>`_

