:man_page: mongoc_collection_find_and_modify_with_opts

mongoc_collection_find_and_modify_with_opts()
=============================================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_collection_find_and_modify_with_opts (
     mongoc_collection_t *collection,
     const bson_t *query,
     const mongoc_find_and_modify_opts_t *opts,
     bson_t *reply,
     bson_error_t *error);

Parameters
----------

* ``collection``: A :symbol:`mongoc_collection_t`.
* ``query``: A :symbol:`bson:bson_t` containing the query to locate target document(s).
* ``opts``: A :symbol:`find and modify options <mongoc_find_and_modify_opts_t>`. Must not be NULL.
* ``reply``: A |bson_t-opt-storage-ptr| to contain the results.
* ``error``: An optional location for a :symbol:`bson_error_t <errors>` or ``NULL``.

Description
-----------

Update and return an object.

``reply`` is always initialized, and must be freed with :symbol:`bson:bson_destroy()`.

If an unacknowledged write concern is set (through :symbol:`mongoc_find_and_modify_opts_append`), the output ``reply`` is always an empty document.

On success, the output ``reply`` contains the full server reply to the ``findAndModify`` command. See the `MongoDB Manual page for findAndModify <https://www.mongodb.com/docs/manual/reference/command/findAndModify/#output>`_ for the expected server reply.

Errors
------

Errors are propagated via the ``error`` parameter.

Returns
-------

Returns ``true`` if successful. Returns ``false`` and sets ``error`` if there are invalid arguments or a server or network error.

A write concern timeout or write concern error is considered a failure.

Example
-------

See the example code for :ref:`mongoc_find_and_modify_opts_t <mongoc_collection_find_and_modify_with_opts_example>`.

