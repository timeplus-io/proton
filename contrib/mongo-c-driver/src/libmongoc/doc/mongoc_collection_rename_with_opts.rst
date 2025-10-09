:man_page: mongoc_collection_rename_with_opts

mongoc_collection_rename_with_opts()
====================================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_collection_rename_with_opts (mongoc_collection_t *collection,
                                      const char *new_db,
                                      const char *new_name,
                                      bool drop_target_before_rename,
                                      const bson_t *opts,
                                      bson_error_t *error);

Parameters
----------

* ``collection``: A :symbol:`mongoc_collection_t`.
* ``new_db``: The name of the new database.
* ``new_name``: The new name for the collection.
* ``drop_target_before_rename``: If an existing collection matches the new name, drop it before the rename.
* ``error``: An optional location for a :symbol:`bson_error_t <errors>` or ``NULL``.

.. |opts-source| replace:: ``collection``

``opts`` may be NULL or a BSON document with additional command options:

* ``writeConcern``: Construct a :symbol:`mongoc_write_concern_t` and use :symbol:`mongoc_write_concern_append` to add the write concern to ``opts``. See the example code for :symbol:`mongoc_client_write_command_with_opts`.
* ``sessionId``: First, construct a :symbol:`mongoc_client_session_t` with :symbol:`mongoc_client_start_session`. You can begin a transaction with :symbol:`mongoc_client_session_start_transaction`, optionally with a :symbol:`mongoc_transaction_opt_t` that overrides the options inherited from |opts-source|, and use :symbol:`mongoc_client_session_append` to add the session to ``opts``. See the example code for :symbol:`mongoc_client_session_t`.
* ``serverId``: To target a specific server, include an int32 "serverId" field. Obtain the id by calling :symbol:`mongoc_client_select_server`, then :symbol:`mongoc_server_description_id` on its return value.

Description
-----------

This function is a helper to rename an existing collection on a MongoDB server. The name of the collection will also be updated internally so it is safe to continue using this collection after the rename. Additional operations will occur on renamed collection.

If no write concern is provided in ``opts``, the collection's write concern is used.

Errors
------

Errors are propagated via the ``error`` parameter.

Returns
-------

Returns ``true`` if successful. Returns ``false`` and sets ``error`` if there are invalid arguments or a server or network error.

