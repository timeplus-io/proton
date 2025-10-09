:man_page: mongoc_session_opts_set_snapshot

mongoc_session_opts_set_snapshot()
==================================

Synopsis
--------

.. code-block:: c

  void
  mongoc_session_opts_set_snapshot (mongoc_session_opt_t *opts,
                                    bool snapshot);

Configure snapshot reads for a session. If true (false by default), each read operation in the session will be sent with a "snapshot" level read concern. After the first read operation ("find", "aggregate" or "distinct"), subsequent read operations will read from the same point in time as the first read operation. Set to true to enable snapshot reads. See `the official documentation for Read Concern "snapshot" <https://www.mongodb.com/docs/manual/reference/read-concern-snapshot/>`_.

Snapshot reads and causal consistency are mutually exclusive. Attempting to set both to true will result in an error. See :symbol:`mongoc_session_opts_set_causal_consistency()`.

Snapshot reads can only be used on MongoDB server version 5.0 and later and cannot be used during a transaction. A write operation in a snapshot-enabled session will also result in an error.

Parameters
----------

* ``opts``: A :symbol:`mongoc_session_opt_t`.
* ``snapshot``: True or false.

Example
-------

.. code-block:: c

   mongoc_client_t *client;
   mongoc_session_opt_t *session_opts;
   mongoc_client_session_t *client_session;
   mongoc_collection_t *collection;
   bson_t query_opts = BSON_INITIALIZER;
   bson_t filter = BSON_INITIALIZER;
   bson_t pipeline = BSON_INITIALIZER;

   client = mongoc_client_new ("mongodb://example/?appname=session-opts-example");
   mongoc_client_set_error_api (client, MONGOC_ERROR_API_VERSION_2);

   session_opts = mongoc_session_opts_new ();
   mongoc_session_opts_set_snapshot (session_opts, true);
   client_session = mongoc_client_start_session (client, session_opts, &error);
   mongoc_session_opts_destroy (session_opts);

   if (!client_session) {
      fprintf (stderr, "Failed to start session: %s\n", error.message);
      abort ();
   }

   collection = mongoc_client_get_collection (client, "test", "collection");
   r = mongoc_client_session_append (client_session, &find_opts, &error);
   if (!r) {
      fprintf (stderr, "mongoc_client_session_append failed: %s\n", error.message);
      abort ();
   }

   /* First read operation will set the snapshot time for subsequent reads. */
   cursor = mongoc_collection_find_with_opts (collection, filter, &query_opts, NULL);

   /* Subsequent read operations will automatically read from the same point
    * in time as the first read operation. */
   cursor = mongoc_collection_aggregate (collection, MONGOC_QUERY_NONE, pipeline, &query_opts, NULL);

.. only:: html

  .. include:: includes/seealso/session.txt