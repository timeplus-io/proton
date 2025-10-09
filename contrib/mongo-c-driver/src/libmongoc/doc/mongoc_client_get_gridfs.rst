:man_page: mongoc_client_get_gridfs

mongoc_client_get_gridfs()
==========================

Synopsis
--------

.. code-block:: c

  mongoc_gridfs_t *
  mongoc_client_get_gridfs (mongoc_client_t *client,
                            const char *db,
                            const char *prefix,
                            bson_error_t *error) BSON_GNUC_WARN_UNUSED_RESULT;

The ``mongoc_client_get_gridfs()`` function shall create a new :symbol:`mongoc_gridfs_t`. The ``db`` parameter is the name of the database which the gridfs instance should exist in. The ``prefix`` parameter corresponds to the gridfs collection namespacing; its default is "fs", thus the default GridFS collection names are "fs.files" and "fs.chunks".

Parameters
----------

* ``client``: A :symbol:`mongoc_client_t`.
* ``db``: The database name.
* ``prefix``: Optional prefix for gridfs collection names or ``NULL``.
* ``error``: An optional location for a :symbol:`bson_error_t <errors>` or ``NULL``.

Errors
------

Errors are propagated via the ``error`` parameter.

Returns
-------

On success, returns a :symbol:`mongoc_gridfs_t` you must free with :symbol:`mongoc_gridfs_destroy()`. Returns ``NULL`` upon failure and sets ``error``.
