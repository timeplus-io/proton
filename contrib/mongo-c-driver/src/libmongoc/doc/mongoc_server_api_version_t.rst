:man_page: mongoc_server_api_version_t

mongoc_server_api_version_t
===========================

A representation of server API version numbers.

Synopsis
--------

Used to specify which version of the MongoDB server's API to use for driver connections.

Supported API Versions
----------------------

The driver currently supports the following MongoDB API versions:

====================  ======================
Enum value            MongoDB version string
====================  ======================
MONGOC_SERVER_API_V1  "1"
====================  ======================

.. only:: html

  Functions
  ---------

  .. toctree::
    :titlesonly:
    :maxdepth: 1

    mongoc_server_api_version_from_string
    mongoc_server_api_version_to_string
