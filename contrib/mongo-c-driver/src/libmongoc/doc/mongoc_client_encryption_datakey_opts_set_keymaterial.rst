:man_page: mongoc_client_encryption_datakey_opts_set_keymaterial

mongoc_client_encryption_datakey_opts_set_keymaterial()
=======================================================

Synopsis
--------

.. code-block:: c

   void
   mongoc_client_encryption_datakey_opts_set_keymaterial (
      mongoc_client_encryption_datakey_opts_t *opts,
      const uint8_t *data,
      uint32_t len);

Sets the custom key material to be used by the data key for encryption and decryption.

Parameters
----------

* ``opts``: A :symbol:`mongoc_client_encryption_datakey_opts_t`
* ``data``: A pointer to the bytes constituting the custom key material.
* ``len``: The length of the bytes constituting the custom key material.

Description
-----------

Key material is used to encrypt and decrypt data. If custom key material is not provided, the key material for the new data key is generated from a cryptographically secure random device.
