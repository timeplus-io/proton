:man_page: mongoc_database_aggregate

mongoc_database_aggregate()
===========================

Synopsis
--------

.. code-block:: c

  mongoc_cursor_t *
  mongoc_database_aggregate (mongoc_database_t *database,
                             const bson_t *pipeline,
                             const bson_t *opts,
                             const mongoc_read_prefs_t *read_prefs)
     BSON_GNUC_WARN_UNUSED_RESULT;

Parameters
----------

* ``database``: A :symbol:`mongoc_database_t`.
* ``pipeline``: A :symbol:`bson:bson_t`, either a BSON array or a BSON document containing an array field named "pipeline".
* ``opts``: A :symbol:`bson:bson_t` containing options for the command, or ``NULL``.
* ``read_prefs``: A :symbol:`mongoc_read_prefs_t` or ``NULL``.

.. |opts-source| replace:: ``database``

.. include:: includes/aggregate-opts.txt

For a list of all options, see `the MongoDB Manual entry on the aggregate command <https://www.mongodb.com/docs/manual/reference/command/aggregate/>`_.

Description
-----------

This function creates a cursor which sends the aggregate command on the underlying database upon the first call to :symbol:`mongoc_cursor_next()`. For more information on building aggregation pipelines, see `the MongoDB Manual entry on the aggregate command <https://www.mongodb.com/docs/manual/reference/command/aggregate/>`_. Note that the pipeline must start with a compatible stage that does not require an underlying collection (e.g. "$currentOp", "$listLocalSessions").

Read preferences, read and write concern, and collation can be overridden by various sources. The highest-priority sources for these options are listed first in the following table. In a transaction, read concern and write concern are prohibited in ``opts`` and the read preference must be primary or NULL. Write concern is applied from ``opts``, or if ``opts`` has no write concern and the aggregation pipeline includes "$out", the write concern is applied from ``database``.

================== ============== ============== =========
Read Preferences   Read Concern   Write Concern  Collation
================== ============== ============== =========
``read_prefs``     ``opts``       ``opts``       ``opts``
Transaction        Transaction    Transaction
``database``       ``database``   ``database``
================== ============== ============== =========

:ref:`See the example for transactions <mongoc_client_session_start_transaction_example>` and for :ref:`the "distinct" command with opts <mongoc_client_read_command_with_opts_example>`.

.. include:: includes/retryable-read-aggregate.txt

Returns
-------

.. include:: includes/returns-cursor.txt

Example
-------

.. code-block:: c

  #include <bson/bson.h>
  #include <mongoc/mongoc.h>

  static mongoc_cursor_t *
  current_op_query (mongoc_client_t *client)
  {
     mongoc_cursor_t *cursor;
     mongoc_database_t *database;
     bson_t *pipeline;

     pipeline = BCON_NEW ("pipeline",
                          "[",
                          "{",
                          "$currentOp",
                          "{",
                          "}",
                          "}",
                          "]");

     /* $currentOp must be run on the admin database */
     database = mongoc_client_get_database (client, "admin");

     cursor = mongoc_database_aggregate (
        database, pipeline, NULL, NULL);

     bson_destroy (pipeline);
     mongoc_database_destroy (database);

     return cursor;
  }
