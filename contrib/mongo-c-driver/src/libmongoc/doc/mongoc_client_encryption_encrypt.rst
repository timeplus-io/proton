:man_page: mongoc_client_encryption_encrypt

mongoc_client_encryption_encrypt()
==================================

Synopsis
--------

.. code-block:: c

   bool
   mongoc_client_encryption_encrypt (
      mongoc_client_encryption_t *client_encryption,
      const bson_value_t *value,
      mongoc_client_encryption_encrypt_opts_t *opts,
      bson_value_t *ciphertext,
      bson_error_t *error);

Performs explicit encryption.

``ciphertext`` is always initialized (even on failure). Caller must call :symbol:`bson_value_destroy()` to free.

To insert or query with an "Indexed" or "RangePreview" encrypted payload, use a
:symbol:`mongoc_client_t` configured with
:symbol:`mongoc_auto_encryption_opts_t`. The
:symbol:`mongoc_auto_encryption_opts_t` may be configured to bypass query
analysis with :symbol:`mongoc_auto_encryption_opts_set_bypass_query_analysis`.
The :symbol:`mongoc_auto_encryption_opts_t` must not be configured to bypass
automatic encryption with
:symbol:`mongoc_auto_encryption_opts_set_bypass_auto_encryption`. **Note** that
the ``"RangePreview"`` payload type |qenc:is-experimental|. The |qenc:range-is-experimental| 

To insert with a ``RangePreview`` payload 
:symbol:`mongoc_client_encryption_encrypt_range_opts_t` must be set in ``opts``.

To query with a ``RangePreview`` payload, use :symbol:`mongoc_client_encryption_encrypt_expression()`

**NOTE** that the |qenc:range-is-experimental|

Parameters
----------

* ``client_encryption``: A :symbol:`mongoc_client_encryption_t`
* ``value``: The value to encrypt.
* ``opts``: A :symbol:`mongoc_client_encryption_encrypt_opts_t`.
* ``ciphertext``: A :symbol:`bson_value_t` for the resulting ciphertext (a BSON binary with subtype 6).
* ``error``: A :symbol:`bson_error_t` set on failure.

Returns
-------

Returns ``true`` if successful. Returns ``false`` and sets ``error`` otherwise.

.. seealso::

  | :symbol:`mongoc_client_encryption_encrypt_opts_t`

  | :symbol:`mongoc_client_enable_auto_encryption()`

  | :symbol:`mongoc_client_encryption_decrypt()`

  | :symbol:`mongoc_client_encryption_encrypt_expression()`
