:man_page: mongoc_server_api_strict

mongoc_server_api_strict()
==========================

Synopsis
--------

.. code-block:: c

  void
  mongoc_server_api_strict (mongoc_server_api_t *api, bool strict);

Set whether to be strict about the list of allowed commands in this API version.

Parameters
----------

* ``api``: A :symbol:`mongoc_server_api_t`.
* ``strict``: Whether or not to be struct about the list of allowed API commands.
