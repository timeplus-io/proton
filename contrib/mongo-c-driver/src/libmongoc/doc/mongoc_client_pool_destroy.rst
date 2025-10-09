:man_page: mongoc_client_pool_destroy

mongoc_client_pool_destroy()
============================

Synopsis
--------

.. code-block:: c

  void
  mongoc_client_pool_destroy (mongoc_client_pool_t *pool);

Release all resources associated with ``pool``, including freeing the structure.

All :symbol:`mongoc_client_t` objects obtained from :symbol:`mongoc_client_pool_pop()` from ``pool`` must be pushed back onto the pool with :symbol:`mongoc_client_pool_push()` prior to calling :symbol:`mongoc_client_pool_destroy()`.

This method is NOT thread safe, and must only be called by one thread. It should be called once the application is shutting down, and after all other threads that use clients have been joined.

Parameters
----------

* ``pool``: A :symbol:`mongoc_client_pool_t`.

