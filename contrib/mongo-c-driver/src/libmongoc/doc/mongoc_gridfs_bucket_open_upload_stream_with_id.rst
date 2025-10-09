:man_page: mongoc_gridfs_bucket_open_upload_stream_with_id

mongoc_gridfs_bucket_open_upload_stream_with_id()
=================================================

Synopsis
--------

.. code-block:: c

  mongoc_stream_t *
  mongoc_gridfs_bucket_open_upload_stream_with_id (mongoc_gridfs_bucket_t *bucket,
                                                   const bson_value_t *file_id,
                                                   const char *filename,
                                                   const bson_t *opts,
                                                   bson_error_t *error)
     BSON_GNUC_WARN_UNUSED_RESULT;

Parameters
----------

* ``bucket``: A :symbol:`mongoc_gridfs_bucket_t`.
* ``file_id``: A :symbol:`bson_value_t` specifying the id of the created file.
* ``filename``: The name of the file to create.
* ``opts``: A :symbol:`bson_t` or ``NULL``.
* ``error``: A :symbol:`bson_error_t` to receive any error or ``NULL``.

.. include:: includes/gridfs-bucket-upload-opts.txt

Description
-----------

Opens a stream for writing to a new file in GridFS for a specified file id.
To have libmongoc generate an id, use :symbol:`mongoc_gridfs_bucket_open_upload_stream()`.

Returns
-------

A :symbol:`mongoc_stream_t` that can be written to or ``NULL`` on failure. Errors on this stream can be retrieved with :symbol:`mongoc_gridfs_bucket_stream_error`. After calling :symbol:`mongoc_stream_close` the file is completely written in GridFS.

.. seealso::

  | :symbol:`mongoc_gridfs_bucket_open_upload_stream()`

  | :symbol:`mongoc_gridfs_bucket_stream_error()`

