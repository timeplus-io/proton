:man_page: mongoc_gridfs_find_one_by_filename

mongoc_gridfs_find_one_by_filename()
====================================

Synopsis
--------

.. code-block:: c

  mongoc_gridfs_file_t *
  mongoc_gridfs_find_one_by_filename (mongoc_gridfs_t *gridfs,
                                      const char *filename,
                                      bson_error_t *error)
     BSON_GNUC_WARN_UNUSED_RESULT;

Parameters
----------

* ``gridfs``: A :symbol:`mongoc_gridfs_t`.
* ``filename``: A UTF-8 encoded string containing the filename.
* ``error``: An optional location for a :symbol:`bson_error_t <errors>` or ``NULL``.

Description
-----------

Finds the first file matching the filename specified. If there is an error, NULL is returned and ``error`` is filled out; if there is no error but no matching file is found, NULL is returned and the error code and domain are 0.

.. include:: includes/retryable-read.txt

Errors
------

Errors are propagated via the ``error`` parameter.

Returns
-------

Returns a newly allocated :symbol:`mongoc_gridfs_file_t` if successful. You must free the resulting file with :symbol:`mongoc_gridfs_file_destroy()` when no longer in use.
