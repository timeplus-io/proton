:man_page: mongoc_server_api_t

mongoc_server_api_t
===================

A versioned API to use for connections.

Synopsis
--------

Used to specify which version of the MongoDB server's API to use for driver connections.

The server API type takes a :symbol:`mongoc_server_api_version_t`. It can optionally be strict about the list of allowed commands in that API version, and can also optionally provide errors for deprecated commands in that API version.

A :symbol:`mongoc_server_api_t` can be set on a client, and will then be sent to MongoDB for most commands run using that client.

.. only:: html

  Functions
  ---------

  .. toctree::
    :titlesonly:
    :maxdepth: 1

    mongoc_server_api_copy
    mongoc_server_api_deprecation_errors
    mongoc_server_api_destroy
    mongoc_server_api_get_deprecation_errors
    mongoc_server_api_get_strict
    mongoc_server_api_get_version
    mongoc_server_api_new
    mongoc_server_api_strict
