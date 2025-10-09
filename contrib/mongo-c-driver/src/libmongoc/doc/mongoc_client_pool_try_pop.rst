:man_page: mongoc_client_pool_try_pop

mongoc_client_pool_try_pop()
============================

Synopsis
--------

.. code-block:: c

  mongoc_client_t *
  mongoc_client_pool_try_pop (mongoc_client_pool_t *pool)
     BSON_GNUC_WARN_UNUSED_RESULT;

This function is identical to :symbol:`mongoc_client_pool_pop()` except it will return ``NULL`` instead of blocking for a client to become available.

.. note::

   Return a checked out :symbol:`mongoc_client_t` to the pool with :symbol:`mongoc_client_pool_push` quickly to encourage reuse of clients among threads.

Parameters
----------

* ``pool``: A :symbol:`mongoc_client_pool_t`.

Returns
-------

A :symbol:`mongoc_client_t` if one is immediately available, otherwise ``NULL``.

.. include:: includes/mongoc_client_pool_thread_safe.txt
