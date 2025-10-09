:man_page: mongoc_collection_create_indexes_with_opts

mongoc_collection_create_indexes_with_opts()
============================================

Synopsis
--------

.. code-block:: c

   typedef struct _mongoc_index_model_t mongoc_index_model_t;
 
   mongoc_index_model_t *
   mongoc_index_model_new (const bson_t *keys, const bson_t *opts);
 
   void mongoc_index_model_destroy (mongoc_index_model_t *model);
 
   bool
   mongoc_collection_create_indexes_with_opts (mongoc_collection_t *collection,
                                               mongoc_index_model_t **models,
                                               size_t n_models,
                                               const bson_t *opts,
                                               bson_t *reply,
                                               bson_error_t *error);

Parameters
----------

* ``collection``: A :symbol:`mongoc_collection_t`.
* ``models``: An array of ``mongoc_index_model_t *``.
* ``n_models``: The number of ``models``.
* ``opts``: Optional options.
* ``reply``: A |bson_t-opt-storage-ptr| to contain the results.
* ``error``: An optional location for a :symbol:`bson_error_t <errors>` or ``NULL``.

.. |opts-source| replace:: ``collection``

.. include:: includes/write-opts.txt

Additional options passed in ``opts`` are appended to the ``createIndexes`` command. See the `MongoDB Manual for createIndexes <https://www.mongodb.com/docs/manual/reference/command/createIndexes/>`_ for all supported options.

If no write concern is provided in ``opts``, the collection's write concern is used.

mongoc_index_model_t
````````````````````
Each ``mongoc_index_model_t`` represents an index to create. ``mongoc_index_model_new`` includes:

* ``keys`` Expected to match the form of the ``key`` field in the `createIndexes <https://www.mongodb.com/docs/manual/reference/command/createIndexes/>`_ command.
* ``opts`` Optional index options appended as a sibling to the ``key`` field in the `createIndexes <https://www.mongodb.com/docs/manual/reference/command/createIndexes/>`_ command.


Description
-----------

This function wraps around the `createIndexes <https://www.mongodb.com/docs/manual/reference/command/createIndexes/>`_ command.

Errors
------

Errors are propagated via the ``error`` parameter.

Returns
-------

Returns ``true`` if successful. Returns ``false`` and sets ``error`` if there are invalid arguments or a server or network error.

.. seealso::

  | `Manage Collection Indexes <manage-collection-indexes_>`_.
