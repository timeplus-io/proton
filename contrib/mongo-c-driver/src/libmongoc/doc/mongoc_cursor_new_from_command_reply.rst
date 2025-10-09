:man_page: mongoc_cursor_new_from_command_reply

mongoc_cursor_new_from_command_reply()
======================================

.. warning::
   .. deprecated:: 1.11.0

      This function is deprecated and should not be used in new code.

      Please use :symbol:`mongoc_cursor_new_from_command_reply_with_opts()` in new code.

      When migrating from the deprecated :symbol:`mongoc_cursor_new_from_command_reply()` to :symbol:`mongoc_cursor_new_from_command_reply_with_opts()`,
      note that options previously passed to the ``reply`` argument (e.g. "batchSize") must instead be provided in the ``opts`` argument.

Synopsis
--------

.. code-block:: c

  mongoc_cursor_t *
  mongoc_cursor_new_from_command_reply (mongoc_client_t *client,
                                        bson_t *reply,
                                        uint32_t server_id);
     BSON_GNUC_DEPRECATED_FOR (mongoc_cursor_new_from_command_reply_with_opts);

Parameters
----------

* ``client``: A :symbol:`mongoc_client_t`.
* ``reply``: The reply to a command, such as "aggregate", "find", or "listCollections", that returns a cursor document. The reply is destroyed by ``mongoc_cursor_new_from_command_reply`` and must not be accessed afterward.
* ``server_id``: The opaque id of the server used to execute the command.

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

``mongoc_cursor_new_from_command_reply`` is a low-level function that initializes a :symbol:`mongoc_cursor_t` from such a reply. Additional options such as "tailable" or "awaitData" can be included in the reply.

When synthesizing a completed cursor response that has no more batches (i.e. with cursor id 0), ``server_id`` may be 0. If the cursor response is not completed (i.e. with non-zero cursor id), pass the ``server_id`` of the server used to create the cursor.

Use this function only for building a language driver that wraps the C Driver. When writing applications in C, higher-level functions such as :symbol:`mongoc_collection_aggregate` are more appropriate, and ensure compatibility with a range of MongoDB versions.

Returns
-------

A :symbol:`mongoc_cursor_t`. On failure, the cursor's error is set. Check for failure with :symbol:`mongoc_cursor_error`.

