:man_page: mongoc_client_encryption_rewrap_many_datakey

mongoc_client_encryption_rewrap_many_datakey()
==============================================

Synopsis
--------

.. code-block:: c

   bool
   mongoc_client_encryption_rewrap_many_datakey (
      mongoc_client_encryption_t *client_encryption,
      const bson_t *filter,
      const char *provider,
      const bson_t *master_key,
      mongoc_client_encryption_rewrap_many_datakey_result_t *result,
      bson_error_t *error);

Rewraps zero or more data keys in the key vault collection that match the
provided ``filter``.

A ``NULL`` argument for ``filter`` is equivalent to being given an empty
document (match all).

If ``provider`` is ``NULL``, rewraps matching data keys with their current KMS
provider and master key.

If ``provider`` is not ``NULL``, rewraps matching data keys with the new KMS
provider as described by ``master_key``. The ``master_key`` document must
conform to the `Client Side Encryption specification
<https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/client-side-encryption.rst#masterkey>`_.

Parameters
----------

* ``client_encryption``: A :symbol:`mongoc_client_encryption_t`.
* ``filter``: The filter to use when finding data keys to rewrap in the key vault collection.
* ``provider``: The new KMS provider to use to encrypt the data keys, or ``NULL`` to use the current KMS provider(s).
* ``master_key``: The master key fields corresponding to the new KMS provider when ``provider`` is not ``NULL``.
* ``result``: An optional :symbol:`mongoc_client_encryption_rewrap_many_datakey_result_t`.
* ``error``: A :symbol:`bson_error_t` set on failure.

Returns
-------

Returns ``true`` if successful. Returns ``false`` and sets ``error`` otherwise.

.. seealso::

  | :symbol:`mongoc_client_encryption_rewrap_many_datakey_result_t`
