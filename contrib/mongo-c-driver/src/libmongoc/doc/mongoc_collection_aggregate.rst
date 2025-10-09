:man_page: mongoc_collection_aggregate

mongoc_collection_aggregate()
=============================

Synopsis
--------

.. code-block:: c

  mongoc_cursor_t *
  mongoc_collection_aggregate (mongoc_collection_t *collection,
                               mongoc_query_flags_t flags,
                               const bson_t *pipeline,
                               const bson_t *opts,
                               const mongoc_read_prefs_t *read_prefs)
     BSON_GNUC_WARN_UNUSED_RESULT;

Parameters
----------

* ``collection``: A :symbol:`mongoc_collection_t`.
* ``flags``: A :symbol:`mongoc_query_flags_t`. Not all flag values apply. In particular, setting ``MONGOC_QUERY_EXHAUST`` results in an error.
* ``pipeline``: A :symbol:`bson:bson_t`, either a BSON array or a BSON document containing an array field named "pipeline".
* ``opts``: A :symbol:`bson:bson_t` containing options for the command, or ``NULL``.
* ``read_prefs``: A :symbol:`mongoc_read_prefs_t` or ``NULL``.

.. |opts-source| replace:: ``collection``

.. include:: includes/aggregate-opts.txt

For a list of all options, see `the MongoDB Manual entry on the aggregate command <https://www.mongodb.com/docs/manual/reference/command/aggregate/>`_.


.. include:: includes/retryable-read-aggregate.txt

Description
-----------

This function creates a cursor which sends the aggregate command on the underlying collection upon the first call to :symbol:`mongoc_cursor_next()`. For more information on building aggregation pipelines, see `the MongoDB Manual entry on the aggregate command <https://www.mongodb.com/docs/manual/reference/command/aggregate/>`_.

Read preferences, read and write concern, and collation can be overridden by various sources. The highest-priority sources for these options are listed first in the following table. In a transaction, read concern and write concern are prohibited in ``opts`` and the read preference must be primary or NULL. Write concern is applied from ``opts``, or if ``opts`` has no write concern and the aggregation pipeline includes "$out", the write concern is applied from ``collection``. The write concern is omitted for MongoDB before 3.4.

================== ============== ============== =========
Read Preferences   Read Concern   Write Concern  Collation
================== ============== ============== =========
``read_prefs``     ``opts``       ``opts``       ``opts``
Transaction        Transaction    Transaction
``collection``     ``collection`` ``collection``
================== ============== ============== =========

:ref:`See the example for transactions <mongoc_client_session_start_transaction_example>` and for :ref:`the "distinct" command with opts <mongoc_client_read_command_with_opts_example>`.

Returns
-------

.. include:: includes/returns-cursor.txt

Example
-------

.. code-block:: c

  #include <bson/bson.h>
  #include <mongoc/mongoc.h>

  static mongoc_cursor_t *
  pipeline_query (mongoc_collection_t *collection)
  {
     mongoc_cursor_t *cursor;
     bson_t *pipeline;

     pipeline = BCON_NEW ("pipeline",
                          "[",
                          "{",
                          "$match",
                          "{",
                          "foo",
                          BCON_UTF8 ("A"),
                          "}",
                          "}",
                          "{",
                          "$match",
                          "{",
                          "bar",
                          BCON_BOOL (false),
                          "}",
                          "}",
                          "]");

     cursor = mongoc_collection_aggregate (
        collection, MONGOC_QUERY_NONE, pipeline, NULL, NULL);

     bson_destroy (pipeline);

     return cursor;
  }

Other Parameters
----------------

When using ``$out``, the pipeline stage that writes, the write_concern field of the :symbol:`mongoc_cursor_t` will be set to the :symbol:`mongoc_write_concern_t` parameter, if it is valid, and applied to the write command when :symbol:`mongoc_cursor_next()` is called. Pass any other parameters to the ``aggregate`` command, besides ``pipeline``, as fields in ``opts``:

.. code-block:: c

  mongoc_write_concern_t *write_concern = mongoc_write_concern_new ();
  mongoc_write_concern_set_w (write_concern, 3);

  pipeline =
     BCON_NEW ("pipeline", "[", "{", "$out", BCON_UTF8 ("collection2"), "}", "]");

  opts = BCON_NEW ("bypassDocumentValidation", BCON_BOOL (true));
  mongoc_write_concern_append (write_concern, opts);

  cursor = mongoc_collection_aggregate (
     collection1, MONGOC_QUERY_NONE, pipeline, opts, NULL);

