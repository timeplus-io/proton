:man_page: mongoc_collection_insert_many

mongoc_collection_insert_many()
===============================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_collection_insert_many (mongoc_collection_t *collection,
                                 const bson_t **documents,
                                 size_t n_documents,
                                 const bson_t *opts,
                                 bson_t *reply,
                                 bson_error_t *error);

Parameters
----------

* ``collection``: A :symbol:`mongoc_collection_t`.
* ``documents``: An array of pointers to :symbol:`bson:bson_t`.
* ``n_documents``: The length of ``documents``.
* ``reply``: A |bson_t-opt-storage-ptr| to contain the results.
* ``error``: An optional location for a :symbol:`bson_error_t <errors>` or ``NULL``.

.. |opts-source| replace:: ``collection``

.. include:: includes/insert-many-opts.txt

Description
-----------

Insert ``documents`` into ``collection``.

To insert a single document, see :symbol:`mongoc_collection_insert_one`.

For any document that does not have an "_id" field, a :symbol:`bson:bson_oid_t` will be generated locally and added to the document. If you must know the inserted document's ``_id``, generate it in your code and include it in the ``document``. The ``_id`` you generate can be a :symbol:`bson:bson_oid_t` or any other non-array BSON type.

If you pass a non-NULL ``reply``, it is filled out with an "insertedCount" field. If there is a server error then ``reply`` may contain a "writeErrors" array and/or a "writeConcernErrors" array (see `Bulk Write Operations <bulk_>`_ for examples). The reply must be freed with :symbol:`bson:bson_destroy`.

Errors
------

Errors are propagated via the ``error`` parameter.

Returns
-------

Returns ``true`` if successful. Returns ``false`` and sets ``error`` if there are invalid arguments or a server or network error.

A write concern timeout or write concern error is considered a failure.

