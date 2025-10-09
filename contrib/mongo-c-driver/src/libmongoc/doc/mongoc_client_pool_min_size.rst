:man_page: mongoc_client_pool_min_size

mongoc_client_pool_min_size()
=============================

.. warning::
   .. deprecated:: 1.9.0

      This function is deprecated because its behavior does not match what developers expect from a "minimum pool size", and its actual behavior is likely to hurt performance.

      Applications should not call this function, they should instead accept the default behavior, which is to keep all idle clients that are pushed into the pool.

Synopsis
--------

.. code-block:: c

  void
  mongoc_client_pool_min_size (mongoc_client_pool_t *pool,
                               uint32_t min_pool_size)
     BSON_GNUC_DEPRECATED;

This function sets the *maximum* number of idle clients to be kept in the pool. Any idle clients in excess of the maximum are destroyed.

Parameters
----------

* ``pool``: A :symbol:`mongoc_client_pool_t`.
* ``min_pool_size``: The number of idle clients to keep in the pool.

.. include:: includes/mongoc_client_pool_thread_safe.txt

Subsequent calls to :symbol:`mongoc_client_pool_push` respect the new minimum size, and close the least recently used :symbol:`mongoc_client_t` if the minimum size is exceeded.
