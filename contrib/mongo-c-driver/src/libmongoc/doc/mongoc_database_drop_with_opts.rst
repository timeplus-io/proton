:man_page: mongoc_database_drop_with_opts

mongoc_database_drop_with_opts()
================================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_database_drop_with_opts (mongoc_database_t *database,
                                  const bson_t *opts,
                                  bson_error_t *error);

Parameters
----------

* ``database``: A :symbol:`mongoc_database_t`.
* ``error``: An optional location for a :symbol:`bson_error_t <errors>` or ``NULL``.

.. |opts-source| replace:: ``database``

.. include:: includes/write-opts.txt

Description
-----------

This function attempts to drop a database on the MongoDB server.

If no write concern is provided in ``opts``, the database's write concern is used.

The ``encryptedFields`` document in ``opts`` may be used to drop a collection used for `Queryable Encryption <queryable-encryption_>`_.

Errors
------

Errors are propagated via the ``error`` parameter.

Returns
-------

Returns ``true`` if successful. Returns ``false`` and sets ``error`` if there are invalid arguments or a server or network error.

