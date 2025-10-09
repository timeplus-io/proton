:man_page: mongoc_client_encryption_get_crypt_shared_version

mongoc_client_encryption_get_crypt_shared_version()
===================================================

Synopsis
--------

.. code-block:: c

  const char*
  mongoc_client_encryption_get_crypt_shared_version (mongoc_client_encryption_t const *enc)
      BSON_GNUC_WARN_UNUSED_RESULT;

Obtain the version string of the crypt_shared_ that is loaded for the given
explicit encryption object. If no crypt_shared_ library is loaded, the returned
pointer will be ``NULL``.

Parameters
----------

* ``enc``: A live :symbol:`mongoc_client_encryption_t`

Returns
-------

A pointer to a null-terminated character array string designating the version of
crypt_shared_ that was loaded for ``enc``. If no crypt_shared_ library is
loaded, the returned pointer will be ``NULL``. The pointed-to array must not be
modified or freed. The returned pointer is only valid for the lifetime of
``enc``.

.. _crypt_shared: https://www.mongodb.com/docs/manual/core/queryable-encryption/reference/shared-library/

.. seealso::

  - :symbol:`mongoc_client_get_crypt_shared_version`
