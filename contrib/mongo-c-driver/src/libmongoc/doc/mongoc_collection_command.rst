:man_page: mongoc_collection_command

mongoc_collection_command()
===========================

Synopsis
--------

.. code-block:: c

  mongoc_cursor_t *
  mongoc_collection_command (mongoc_collection_t *collection,
                             mongoc_query_flags_t flags,
                             uint32_t skip,
                             uint32_t limit,
                             uint32_t batch_size,
                             const bson_t *command,
                             const bson_t *fields,
                             const mongoc_read_prefs_t *read_prefs)
     BSON_GNUC_WARN_UNUSED_RESULT;

This function is superseded by :symbol:`mongoc_collection_command_with_opts()`, :symbol:`mongoc_collection_read_command_with_opts()`, :symbol:`mongoc_collection_write_command_with_opts()`, and :symbol:`mongoc_collection_read_write_command_with_opts()`.

.. include:: includes/not-retryable-read.txt

Parameters
----------

* ``collection``: A :symbol:`mongoc_collection_t`.
* ``flags``: A :symbol:`mongoc_query_flags_t`.
* ``skip``: A uint32_t with the number of documents to skip or zero.
* ``limit``: A uint32_t with the max number of documents to return or zero.
* ``batch_size``: A uint32_t with the number of documents in each batch or zero. Default is 100.
* ``command``: A :symbol:`bson:bson_t` containing the command to execute.
* ``fields``: A :symbol:`bson:bson_t` containing the fields to return or ``NULL``. Not all commands support this option.
* ``read_prefs``: An optional :symbol:`mongoc_read_prefs_t`. Otherwise, the command uses mode ``MONGOC_READ_PRIMARY``.

Returns
-------

.. include:: includes/returns-cursor.txt
