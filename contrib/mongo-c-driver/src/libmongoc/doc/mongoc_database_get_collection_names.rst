:man_page: mongoc_database_get_collection_names

mongoc_database_get_collection_names()
======================================

.. warning::
   .. deprecated:: 1.9.0

      This function is deprecated and should not be used in new code.

      Please use :symbol:`mongoc_database_get_collection_names_with_opts()` in new code.

Synopsis
--------

.. code-block:: c

  char **
  mongoc_database_get_collection_names (mongoc_database_t *database,
                                        bson_error_t *error)
     BSON_GNUC_WARN_UNUSED_RESULT
     BSON_GNUC_DEPRECATED_FOR (mongoc_database_get_collection_names_with_opts);

Description
-----------

Fetches a ``NULL`` terminated array of ``NULL-byte`` terminated ``char*`` strings containing the names of all of the collections in ``database``.

.. include:: includes/retryable-read.txt

Parameters
----------

* ``database``: A :symbol:`mongoc_database_t`.
* ``error``: An optional location for a :symbol:`bson_error_t <errors>` or ``NULL``.

Errors
------

Errors are propagated via the ``error`` parameter.

Returns
-------

A ``NULL`` terminated array of ``NULL`` terminated ``char*`` strings that should be freed with :symbol:`bson:bson_strfreev()`. Upon failure, ``NULL`` is returned and ``error`` is set.
