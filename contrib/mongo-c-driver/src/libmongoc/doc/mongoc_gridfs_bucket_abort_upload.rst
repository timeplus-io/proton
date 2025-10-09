:man_page: mongoc_gridfs_bucket_abort_upload

mongoc_gridfs_bucket_abort_upload()
===================================

Synopsis
--------

.. code-block:: c

   bool
   mongoc_gridfs_bucket_abort_upload (mongoc_stream_t *stream);

Parameters
----------

* ``stream``: A :symbol:`mongoc_stream_t` created by :symbol:`mongoc_gridfs_bucket_open_upload_stream` or :symbol:`mongoc_gridfs_bucket_open_upload_stream_with_id`.

Description
-----------

Aborts the upload of a GridFS upload stream.

Returns
-------

True on success. False otherwise, and sets an error on ``stream``.

.. seealso::

  | :symbol:`mongoc_gridfs_bucket_open_upload_stream`

  | :symbol:`mongoc_gridfs_bucket_open_upload_stream_with_id()`

