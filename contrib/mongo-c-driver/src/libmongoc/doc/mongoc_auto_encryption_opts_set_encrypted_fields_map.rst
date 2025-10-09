:man_page: mongoc_auto_encryption_opts_set_encrypted_fields_map

mongoc_auto_encryption_opts_set_encrypted_fields_map()
======================================================

Synopsis
--------

.. code-block:: c

   void
   mongoc_auto_encryption_opts_set_encrypted_fields_map (
      mongoc_auto_encryption_opts_t *opts, const bson_t *encrypted_fields_map);

.. versionadded:: 1.22.0

Parameters
----------

* ``opts``: The :symbol:`mongoc_auto_encryption_opts_t`
* ``encrypted_fields_map``: A :symbol:`bson_t` where keys are collection namespaces and values are encrypted fields documents.

Supplying an ``encrypted_fields_map`` provides more security than relying on an ``encryptedFields`` obtained from the server. It protects against a malicious server advertising a false ``encryptedFields``.

The schema map does not apply to collections encrypted with: `Queryable Encryption <queryable-encryption_>`_. See :symbol:`mongoc_auto_encryption_opts_set_schema_map()` instead.

.. seealso::

  | :symbol:`mongoc_client_enable_auto_encryption()`

  | `In-Use Encryption <in-use-encryption_>`_

