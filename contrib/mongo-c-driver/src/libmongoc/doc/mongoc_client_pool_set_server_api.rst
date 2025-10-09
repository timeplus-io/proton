:man_page: mongoc_client_pool_set_server_api

mongoc_client_pool_set_server_api()
===================================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_client_pool_set_server_api (mongoc_client_pool_t *pool,
                                     const mongoc_server_api_t *api,
                                     bson_error_t *error);

Set the API version to use for clients created through ``pool``. Once the API version is set on a pool, it may not be changed to a new value. Attempting to do so will cause this method to fail and set ``error``.

Parameters
----------

* ``pool``: A :symbol:`mongoc_client_pool_t`.
* ``api``: A :symbol:`mongoc_server_api_t`.
* ``error``: A :symbol:`bson_error_t`.

Returns
-------

True if the version was successfully set, false if not. On failure, ``error`` will be set.
