:man_page: mongoc_gridfs_bucket_delete_by_id

mongoc_gridfs_bucket_delete_by_id()
===================================

Synopsis
--------

.. code-block:: c

   bool
   mongoc_gridfs_bucket_delete_by_id (mongoc_gridfs_bucket_t *bucket,
                                      const bson_value_t *file_id,
                                      bson_error_t *error);

Parameters
----------

* ``bucket``: A :symbol:`mongoc_gridfs_bucket_t`.
* ``file_id``: A :symbol:`bson_value_t` of the id of the file to delete.
* ``error``: A :symbol:`bson_error_t` to receive any error or ``NULL``.

Description
-----------

Deletes a file and its contents from GridFS.

Returns
-------
True if the operation succeeded. False otherwise, and sets ``error``.
