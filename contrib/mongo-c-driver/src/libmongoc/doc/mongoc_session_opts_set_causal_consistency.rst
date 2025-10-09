:man_page: mongoc_session_opts_set_causal_consistency

mongoc_session_opts_set_causal_consistency()
============================================

Synopsis
--------

.. code-block:: c

  void
  mongoc_session_opts_set_causal_consistency (mongoc_session_opt_t *opts,
                                              bool causal_consistency);

Configure causal consistency in a session. If true (the default), each operation in the session will be causally ordered after the previous read or write operation. Set to false to disable causal consistency. See `the MongoDB Manual Entry for Causal Consistency <https://www.mongodb.com/docs/manual/core/read-isolation-consistency-recency/#causal-consistency>`_.

Causal consistency and snapshot reads are mutually exclusive. Attempting to set both to true will result in an error. See :symbol:`mongoc_session_opts_set_snapshot()`.

Unacknowledged writes are not causally consistent. If you execute a write operation with a :symbol:`mongoc_write_concern_t` on which you have called :symbol:`mongoc_write_concern_set_w` with a value of 0, the write does not participate in causal consistency.

Parameters
----------

* ``opts``: A :symbol:`mongoc_session_opt_t`.
* ``causal_consistency``: True or false.

Example
-------

.. code-block:: c

   mongoc_client_t *client;
   mongoc_session_opt_t *session_opts;
   mongoc_client_session_t *client_session;
   mongoc_collection_t *collection;
   bson_t insert_opts = BSON_INITIALIZER;
   bson_t *doc;
   bson_error_t error;
   bool r;

   client = mongoc_client_new ("mongodb://example/?appname=session-opts-example");
   mongoc_client_set_error_api (client, 2);

   session_opts = mongoc_session_opts_new ();
   mongoc_session_opts_set_causal_consistency (session_opts, false);
   client_session = mongoc_client_start_session (client, session_opts, &error);
   mongoc_session_opts_destroy (session_opts);

   if (!client_session) {
      fprintf (stderr, "Failed to start session: %s\n", error.message);
      abort ();
   }

   collection = mongoc_client_get_collection (client, "test", "collection");
   doc = BCON_NEW ("_id", BCON_INT32 (1));
   r = mongoc_client_session_append (client_session, &insert_opts, NULL);
   if (!r) {
      fprintf (stderr, "mongoc_client_session_append failed: %s\n", error.message);
      abort ();
   }

   r = mongoc_collection_insert_one (
      collection, doc, &insert_opts, NULL /* reply */, &error);

   if (!r) {
      fprintf (stderr, "Insert failed: %s\n", error.message);
      abort ();
   }

.. only:: html

  .. include:: includes/seealso/session.txt
