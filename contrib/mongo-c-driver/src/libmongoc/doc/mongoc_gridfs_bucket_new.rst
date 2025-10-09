:man_page: mongoc_gridfs_bucket_new

mongoc_gridfs_bucket_new()
==========================

Synopsis
--------

.. code-block:: c

  mongoc_gridfs_bucket_t *
  mongoc_gridfs_bucket_new (mongoc_database_t *db,
                            const bson_t *opts,
                            const mongoc_read_prefs_t *read_prefs,
                            bson_error_t* error) BSON_GNUC_WARN_UNUSED_RESULT;

Parameters
----------

* ``db``: A :symbol:`mongoc_database_t`.
* ``opts``: A :symbol:`bson_t` or ``NULL``
* ``read_prefs``: A :symbol:`mongoc_read_prefs_t` used for read operations or ``NULL`` to inherit read preferences from ``db``.
* ``error``: A :symbol:`bson_error_t` or ``NULL``.

.. include:: includes/gridfs-bucket-opts.txt

Description
-----------

Creates a new :symbol:`mongoc_gridfs_bucket_t`. Use this handle to perform GridFS operations.

Returns
-------

A newly allocated :symbol:`mongoc_gridfs_bucket_t` that should be freed with :symbol:`mongoc_gridfs_bucket_destroy()` or ``NULL`` on failure.
