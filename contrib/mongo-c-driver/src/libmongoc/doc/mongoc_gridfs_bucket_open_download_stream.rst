:man_page: mongoc_gridfs_bucket_open_download_stream

mongoc_gridfs_bucket_open_download_stream()
===========================================

Synopsis
--------

.. code-block:: c

  mongoc_stream_t *
  mongoc_gridfs_bucket_open_download_stream (mongoc_gridfs_bucket_t *bucket,
                                             const bson_value_t *file_id,
                                             bson_error_t *error)
     BSON_GNUC_WARN_UNUSED_RESULT;

Parameters
----------

* ``bucket``: A :symbol:`mongoc_gridfs_bucket_t`.
* ``file_id``: A :symbol:`bson_value_t` of the id of the file to download.
* ``error``: A :symbol:`bson_error_t` to receive any error or ``NULL``.

Description
-----------

Opens a stream for reading a file from GridFS.

Returns
-------

A :symbol:`mongoc_stream_t` that can be read from or ``NULL`` on failure. Errors on this stream can be retrieved with :symbol:`mongoc_gridfs_bucket_stream_error()`.

.. seealso::

  | :symbol:`mongoc_gridfs_bucket_stream_error()`

