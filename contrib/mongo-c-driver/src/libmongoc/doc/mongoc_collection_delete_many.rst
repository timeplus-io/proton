:man_page: mongoc_collection_delete_many

mongoc_collection_delete_many()
===============================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_collection_delete_many (mongoc_collection_t *collection,
                                 const bson_t *selector,
                                 const bson_t *opts,
                                 bson_t *reply,
                                 bson_error_t *error);

Parameters
----------

* ``collection``: A :symbol:`mongoc_collection_t`.
* ``selector``: A :symbol:`bson:bson_t` containing the query to match documents.
* ``reply``: A |bson_t-opt-storage-ptr| to contain the results.
* ``error``: An optional location for a :symbol:`bson_error_t <errors>` or ``NULL``.

.. |opts-source| replace:: ``collection``

.. include:: includes/delete-many-opts.txt

Description
-----------

This function removes all documents in the given ``collection`` that match ``selector``.

To delete at most one matching document, use :symbol:`mongoc_collection_delete_one`.

If you pass a non-NULL ``reply``, it is filled out with the field "deletedCount". If there is a server error then ``reply`` contains either a "writeErrors" array with one subdocument or a "writeConcernErrors" array. The reply must be freed with :symbol:`bson:bson_destroy`.

Errors
------

Errors are propagated via the ``error`` parameter.

Returns
-------

Returns ``true`` if successful. Returns ``false`` and sets ``error`` if there are invalid arguments or a server or network error.
