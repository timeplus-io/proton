:man_page: mongoc_client_encryption_remove_key_alt_name

mongoc_client_encryption_remove_key_alt_name()
==============================================

Synopsis
--------

.. code-block:: c

   bool
   mongoc_client_encryption_remove_key_alt_name (
      mongoc_client_encryption_t *client_encryption,
      const bson_value_t *keyid,
      const char *keyaltname,
      bson_t *key_doc,
      bson_error_t *error);

Remove ``keyaltname`` from the set of keyAltNames in the key document with UUID ``keyid``.

Also removes the ``keyAltNames`` field from the key document if it would otherwise be empty.

Parameters
----------

* ``client_encryption``: A :symbol:`mongoc_client_encryption_t`.
* ``keyid``: The UUID (BSON binary subtype 0x04) of the key to remove the key alternate name from.
* ``keyaltname``: The key alternate name to remove.
* ``key_doc``: Optional. An uninitialized :symbol:`bson_t` set to the value of the key document before removal of the key alternate name, or an empty document the key does not exist. Must be freed by :symbol:`bson_value_destroy`.
* ``error``: Optional. :symbol:`bson_error_t`.

Returns
-------

Returns ``true`` if successful. Returns ``false`` and sets ``error`` otherwise.

.. seealso::

  | :symbol:`mongoc_client_encryption_t`
