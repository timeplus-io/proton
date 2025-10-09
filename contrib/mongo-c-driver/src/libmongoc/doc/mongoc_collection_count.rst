:man_page: mongoc_collection_count

mongoc_collection_count()
=========================

.. warning::
   .. deprecated:: 1.11.0

      This function is deprecated and should not be used in new code.
      Use :symbol:`mongoc_collection_count_documents` or :symbol:`mongoc_collection_estimated_document_count` instead.

      :symbol:`mongoc_collection_count_documents` has similar performance to calling :symbol:`mongoc_collection_count` with a non-NULL ``query``, and is guaranteed to retrieve an accurate collection count. See :ref:`migrating from deprecated count functions <migrating-from-deprecated-count>` for details.

      :symbol:`mongoc_collection_estimated_document_count` has the same performance as calling :symbol:`mongoc_collection_count` with a NULL ``query``, but is not guaranteed to retrieve an accurate collection count.

.. include:: includes/retryable-read.txt

Synopsis
--------

.. code-block:: c

  int64_t
  mongoc_collection_count (mongoc_collection_t *collection,
                           mongoc_query_flags_t flags,
                           const bson_t *query,
                           int64_t skip,
                           int64_t limit,
                           const mongoc_read_prefs_t *read_prefs,
                           bson_error_t *error)
   BSON_GNUC_DEPRECATED_FOR (mongoc_collection_count_documents or
                             mongoc_collection_estimated_document_count);

Parameters
----------

* ``collection``: A :symbol:`mongoc_collection_t`.
* ``flags``: A :symbol:`mongoc_query_flags_t`.
* ``query``: A :symbol:`bson:bson_t` containing the query.
* ``skip``: A int64_t, zero to ignore.
* ``limit``: A int64_t, zero to ignore.
* ``read_prefs``: A :symbol:`mongoc_read_prefs_t` or ``NULL``.
* ``error``: An optional location for a :symbol:`bson_error_t <errors>` or ``NULL``.

Description
-----------

This function shall execute a count query on the underlying 'collection'. The bson 'query' is not validated, simply passed along as appropriate to the server.  As such, compatibility and errors should be validated in the appropriate server documentation.

For more information, see the `query reference <https://www.mongodb.com/docs/manual/reference/operator/query/>`_ at the MongoDB website.

The :symbol:`mongoc_read_concern_t` specified on the :symbol:`mongoc_collection_t` will be used, if any. If ``read_prefs`` is NULL, the collection's read preferences are used.

Errors
------

Errors are propagated via the ``error`` parameter.

Returns
-------

-1 on failure, otherwise the number of documents counted.

Example
-------

.. code-block:: c

  #include <bson/bson.h>
  #include <mongoc/mongoc.h>
  #include <stdio.h>

  static void
  print_query_count (mongoc_collection_t *collection, bson_t *query)
  {
     bson_error_t error;
     int64_t count;

     count = mongoc_collection_count (
        collection, MONGOC_QUERY_NONE, query, 0, 0, NULL, &error);

     if (count < 0) {
        fprintf (stderr, "Count failed: %s\n", error.message);
     } else {
        printf ("%" PRId64 " documents counted.\n", count);
     }
  }

