:man_page: mongoc_collection_estimated_document_count

mongoc_collection_estimated_document_count()
============================================

Synopsis
--------

.. code-block:: c

   int64_t
   mongoc_collection_estimated_document_count (mongoc_collection_t *collection,
                                               const bson_t *opts,
                                               const mongoc_read_prefs_t *read_prefs,
                                               bson_t *reply,
                                               bson_error_t *error);

Parameters
----------

* ``collection``: A :symbol:`mongoc_collection_t`.
* ``opts``: A :symbol:`bson:bson_t`, ``NULL`` to ignore.
* ``read_prefs``: A :symbol:`mongoc_read_prefs_t` or ``NULL``.
* ``reply``: A |bson_t-opt-storage-ptr| to contain the results.
* ``error``: An optional location for a :symbol:`bson_error_t <errors>` or ``NULL``.

.. |opts-source| replace:: ``collection``

.. include:: includes/read-opts.txt
* ``skip``: An int specifying how many documents matching the ``query`` should be skipped before counting.
* ``limit``: An int specifying the maximum number of documents to count.
* ``comment``: A :symbol:`bson_value_t` specifying the comment to attach to this command. The comment will appear in log messages, profiler output, and currentOp output. Requires MongoDB 4.4 or later.

For a list of all options, see `the MongoDB Manual entry on the count command <https://www.mongodb.com/docs/manual/reference/command/count/>`_.

Description
-----------

This functions executes a count query on ``collection``. In contrast with :symbol:`mongoc_collection_count_documents()`, the count returned is *not* guaranteed to be accurate.

.. include:: includes/retryable-read.txt

Behavior
^^^^^^^^

This method is implemented using the `count <https://www.mongodb.com/docs/manual/reference/command/count/>`_ command. Due to an oversight in versions 5.0.0-5.0.8 of MongoDB, the ``count`` command was not included in version "1" of the Stable API. Applications using this method with the Stable API are recommended to upgrade their server version to 5.0.9+ or disable strict mode (via `:symbol:`mongoc_server_api_strict()`) to avoid encountering errors.

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
  print_count (mongoc_collection_t *collection, bson_t *query)
  {
     bson_error_t error;
     int64_t count;
     bson_t* opts = BCON_NEW ("skip", BCON_INT64(5));

     count = mongoc_collection_estimated_document_count (
        collection, opts, NULL, NULL, &error);
     bson_destroy (opts);

     if (count < 0) {
        fprintf (stderr, "Count failed: %s\n", error.message);
     } else {
        printf ("%" PRId64 " documents counted.\n", count);
     }
  }

.. seealso::

  | :symbol:`mongoc_collection_count_documents()`
  | `Count: Behavior <https://www.mongodb.com/docs/manual/reference/command/count/#behavior>`_ in the MongoDB Manual

