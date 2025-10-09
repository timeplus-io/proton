:man_page: mongoc_gridfs_bucket_stream_error

mongoc_gridfs_bucket_stream_error()
===================================

Synopsis
--------

.. code-block:: c

   bool
   mongoc_gridfs_bucket_stream_error (mongoc_stream_t *stream,
                                      bson_error_t *error);

Parameters
----------

* ``stream``: A :symbol:`mongoc_stream_t` created by :symbol:`mongoc_gridfs_bucket_open_upload_stream`, :symbol:`mongoc_gridfs_bucket_open_upload_stream_with_id`, or :symbol:`mongoc_gridfs_bucket_open_download_stream`.
* ``error``: A :symbol:`bson_error_t` to receive the possible error.

Description
-----------

Retrieves an error for a GridFS stream if one exists.

Returns
-------

True if an error occurred on the stream and sets ``error``. False otherwise.

.. seealso::

  | :symbol:`mongoc_gridfs_bucket_open_upload_stream`

  | :symbol:`mongoc_gridfs_bucket_open_upload_stream_with_id()`

  | :symbol:`mongoc_gridfs_bucket_open_download_stream`

