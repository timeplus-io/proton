:man_page: mongoc_client_encryption_opts_set_key_vault_namespace

mongoc_client_encryption_opts_set_keyvault_namespace()
======================================================

Synopsis
--------

.. code-block:: c

   void
   mongoc_client_encryption_opts_set_keyvault_namespace (
      mongoc_client_encryption_opts_t *opts, const char *db, const char *coll);

Set the database and collection name of the key vault. The key vault is the specially designated collection containing encrypted data keys for `In-Use Encryption <in-use-encryption_>`_.

Parameters
----------

* ``opts``: The :symbol:`mongoc_client_encryption_opts_t`
* ``db``: The database name of the key vault collection.
* ``coll``: The collection name of the key vault collection.

.. seealso::

  | :symbol:`mongoc_client_encryption_new()`

  | `In-Use Encryption <in-use-encryption_>`_

