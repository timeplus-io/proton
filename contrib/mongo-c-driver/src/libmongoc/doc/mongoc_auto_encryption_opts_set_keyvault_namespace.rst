:man_page: mongoc_auto_encryption_opts_set_key_vault_namespace

mongoc_auto_encryption_opts_set_keyvault_namespace()
====================================================

Synopsis
--------

.. code-block:: c

   void
   mongoc_auto_encryption_opts_set_keyvault_namespace (
      mongoc_auto_encryption_opts_t *opts, const char *db, const char *coll);

Set the database and collection name of the key vault. The key vault is the specially designated collection containing encrypted data keys for `In-Use Encryption <in-use-encryption_>`_.

Parameters
----------

* ``opts``: The :symbol:`mongoc_auto_encryption_opts_t`
* ``db``: A ``const char *`` representing the database name of the key vault collection.
* ``coll``: A ``const char *`` representing the collection name of the key vault collection.

.. seealso::

  | :symbol:`mongoc_client_enable_auto_encryption()`

  | `In-Use Encryption <in-use-encryption_>`_

