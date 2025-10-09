:man_page: mongoc_cursor_new_from_command_reply_with_opts

mongoc_cursor_new_from_command_reply_with_opts()
================================================

Synopsis
--------

.. code-block:: c

  mongoc_cursor_t *
  mongoc_cursor_new_from_command_reply_with_opts (mongoc_client_t *client,
                                                  bson_t *reply,
                                                  const bson_t *opts);

Parameters
----------

* ``client``: A :symbol:`mongoc_client_t`.
* ``reply``: The reply to a command, such as "aggregate", "find", or "listCollections", that returns a cursor document. The reply is destroyed by ``mongoc_cursor_new_from_command_reply_with_opts`` and must not be accessed afterward.
* ``opts``: A :symbol:`bson:bson_t`.

``opts`` may be NULL or a BSON document with additional options, which have the same meaning for this function as for :symbol:`mongoc_collection_find_with_opts`:

* ``awaitData``
* ``batchSize``
* ``limit``
* ``maxAwaitTimeMS``
* ``serverId``
* ``sessionId``
* ``skip``
* ``tailable``

Description
-----------

Some MongoDB commands return a "cursor" document. For example, given an "aggregate" command:

.. code-block:: none

  { "aggregate" : "collection", "pipeline" : [], "cursor" : {}}

The server replies:

.. code-block:: none

  {
     "cursor" : {
        "id" : 1234,
        "ns" : "db.collection",
        "firstBatch" : [ ]
     },
     "ok" : 1
  }

``mongoc_cursor_new_from_command_reply_with_opts`` is a low-level function that initializes a :symbol:`mongoc_cursor_t` from such a reply.

When synthesizing a completed cursor response that has no more batches (i.e. with cursor id 0), ``serverId`` may be 0. If the cursor response is not completed (i.e. with non-zero cursor id), pass the ``serverId`` of the server used to create the cursor.

Use this function only for building a language driver that wraps the C Driver. When writing applications in C, higher-level functions such as :symbol:`mongoc_collection_aggregate` are more appropriate, and ensure compatibility with a range of MongoDB versions.

Returns
-------

A :symbol:`mongoc_cursor_t`. On failure, the cursor's error is set. Check for failure with :symbol:`mongoc_cursor_error`.

