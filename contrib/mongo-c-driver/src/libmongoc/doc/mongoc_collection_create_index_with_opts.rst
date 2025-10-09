:man_page: mongoc_collection_create_index_with_opts

mongoc_collection_create_index_with_opts()
==========================================

.. warning::
   .. deprecated:: 1.8.0

      This function is deprecated and should not be used in new code. See `Manage Collection Indexes <manage-collection-indexes_>`_.

Synopsis
--------

.. code-block:: c

  bool
  mongoc_collection_create_index_with_opts (mongoc_collection_t *collection,
                                            const bson_t *keys,
                                            const mongoc_index_opt_t *index_opts,
                                            const bson_t *command_opts,
                                            bson_t *reply,
                                            bson_error_t *error)
     BSON_GNUC_DEPRECATED; 

Parameters
----------

* ``collection``: A :symbol:`mongoc_collection_t`.
* ``keys``: A :symbol:`bson:bson_t`.
* ``index_opts``: A mongoc_index_opt_t.
* ``reply``: A |bson_t-opt-storage-ptr| to contain the results.
* ``error``: An optional location for a :symbol:`bson_error_t <errors>` or ``NULL``.

.. |opts-source| replace:: ``collection``

.. include:: includes/create-index-opts.txt

Description
-----------

This function will request the creation of a new index.

This function will use the ``createIndexes`` command.
The server's reply is stored in ``reply``.

If no write concern is provided in ``command_opts``, the collection's write concern is used.

See :symbol:`mongoc_index_opt_t` for options on creating indexes.

Errors
------

Errors are propagated via the ``error`` parameter.

Returns
-------

Returns ``true`` if successful. Returns ``false`` and sets ``error`` if there are invalid arguments or a server or network error.

``reply`` is always initialized and must be destroyed with :symbol:`bson:bson_destroy()`. If the server is running an obsolete version of MongoDB then ``reply`` may be empty, though it will still be initialized.

