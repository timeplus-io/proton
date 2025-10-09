:man_page: mongoc_gridfs_create_file_from_stream

mongoc_gridfs_create_file_from_stream()
=======================================

Synopsis
--------

.. code-block:: c

  mongoc_gridfs_file_t *
  mongoc_gridfs_create_file_from_stream (mongoc_gridfs_t *gridfs,
                                         mongoc_stream_t *stream,
                                         mongoc_gridfs_file_opt_t *opt)
     BSON_GNUC_WARN_UNUSED_RESULT;

Parameters
----------

* ``gridfs``: A :symbol:`mongoc_gridfs_t`.
* ``stream``: A :symbol:`mongoc_stream_t`.
* ``opt``: A :symbol:`mongoc_gridfs_file_opt_t` to specify file options.

Description
-----------

This function shall create a new :symbol:`mongoc_gridfs_file_t` and fill it with the contents of ``stream``. Note that this function will read from ``stream`` until End of File, making it best suited for file-backed streams.

Returns
-------

A newly allocated :symbol:`mongoc_gridfs_file_t` that should be freed with :symbol:`mongoc_gridfs_file_destroy()` when no longer in use.
Returns NULL and logs an error message if there is a network or server error writing data to the MongoDB server.
