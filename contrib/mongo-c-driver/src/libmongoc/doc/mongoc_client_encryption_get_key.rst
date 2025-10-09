:man_page: mongoc_client_encryption_get_key

mongoc_client_encryption_get_key()
==================================

Synopsis
--------

.. code-block:: c

   bool
   mongoc_client_encryption_get_key (mongoc_client_encryption_t *client_encryption,
                                     const bson_value_t *keyid,
                                     bson_t *key_doc,
                                     bson_error_t *error);

Get a key document in the key vault collection that has the given ``keyid``.

Parameters
----------

* ``client_encryption``: A :symbol:`mongoc_client_encryption_t`.
* ``keyid``: The UUID (BSON binary subtype 0x04) of the key to get.
* ``key_doc``: Optional. An uninitialized :symbol:`bson_t` set to the resulting key document, or an empty document value if the key does not exist. Must be freed by :symbol:`bson_destroy`.
* ``error``: Optional. :symbol:`bson_error_t`.

Returns
-------

Returns ``true`` if successful. Returns ``false`` and sets ``error`` otherwise.

.. seealso::

  | :symbol:`mongoc_client_encryption_t`
