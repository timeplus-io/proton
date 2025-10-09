:man_page: mongoc_client_get_crypt_shared_version

mongoc_client_get_crypt_shared_version()
========================================

Synopsis
--------

.. code-block:: c

  const char *
  mongoc_client_get_crypt_shared_version (const mongoc_client_t *client)
    BSON_GNUC_WARN_UNUSED_RESULT;

Obtain the version string of the crypt_shared_ that is loaded for
auto-encryption on the given ``client``. If no crypt_shared_ library is loaded,
the returned pointer will be ``NULL``.

Parameters
----------

* ``client``: A live :symbol:`mongoc_client_t`

Returns
-------

A pointer to a null-terminated character array string designating the version of
crypt_shared_ that was loaded for auto-encryption with ``client``. If no
crypt_shared_ library is loaded, or auto-encryption is not loaded for the given
``client``, the returned pointer will be ``NULL``. The pointed-to array must not
be modified or freed. The returned pointer is only valid for the lifetime of
``client``.

.. _crypt_shared: https://www.mongodb.com/docs/manual/core/queryable-encryption/reference/shared-library/

.. seealso::

  - :symbol:`mongoc_client_encryption_get_crypt_shared_version`
