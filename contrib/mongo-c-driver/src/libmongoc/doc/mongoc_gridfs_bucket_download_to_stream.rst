:man_page: mongoc_gridfs_bucket_download_to_stream

mongoc_gridfs_bucket_download_to_stream()
=========================================

Synopsis
--------

.. code-block:: c

   bool
   mongoc_gridfs_bucket_download_to_stream (mongoc_gridfs_bucket_t *bucket,
                                            const bson_value_t *file_id,
                                            mongoc_stream_t *destination,
                                            bson_error_t *error);

Parameters
----------

* ``bucket``: A :symbol:`mongoc_gridfs_bucket_t`.
* ``file_id``: A :symbol:`bson_value_t` of the id of the file to download.
* ``destination``: A :symbol:`mongoc_stream_t` which receives data from the downloaded file.
* ``error``: A :symbol:`bson_error_t` to receive any error or ``NULL``.

Description
-----------

Reads from the GridFS file and writes to the ``destination`` stream.

Writes the full contents of the file to the ``destination`` stream.
The ``destination`` stream is not closed after calling :symbol:`mongoc_gridfs_bucket_download_to_stream()`; call :symbol:`mongoc_stream_close()` after.

.. include:: includes/retryable-read.txt

Returns
-------

True if the operation succeeded. False otherwise, and sets ``error``.

.. seealso::

  | :symbol:`mongoc_stream_file_new` and :symbol:`mongoc_stream_file_new_for_path`, which can be used to create a destination stream from a file.

