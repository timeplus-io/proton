:man_page: mongoc_client_encryption_encrypt_opts_set_query_type

mongoc_client_encryption_encrypt_opts_set_query_type()
======================================================

Synopsis
--------

.. code-block:: c

   #define MONGOC_ENCRYPT_QUERY_TYPE_EQUALITY "equality"
   #define MONGOC_ENCRYPT_QUERY_TYPE_RANGEPREVIEW "rangePreview"

   MONGOC_EXPORT (void)
    mongoc_client_encryption_encrypt_opts_set_query_type (
        mongoc_client_encryption_encrypt_opts_t *opts, const char* query_type);

.. versionadded:: 1.22.0

Sets a query type for explicit encryption. Currently, the supported values
for ``query_type`` are ``"equality"`` and ``"rangePreview"``. **NOTE** that the |qenc:range-is-experimental|

Only applies when the algorithm set by :symbol:`mongoc_client_encryption_encrypt_opts_set_algorithm()` is "Indexed" or "RangePreview".
It is an error to set the query type when algorithm is not "Indexed" or "RangePreview".

Parameters
----------

* ``opts``: A :symbol:`mongoc_client_encryption_encrypt_opts_t`
* ``query_type``: A query type to use for explicit encryption.
