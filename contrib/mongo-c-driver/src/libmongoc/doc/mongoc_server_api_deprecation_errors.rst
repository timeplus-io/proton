:man_page: mongoc_server_api_deprecation_errors

mongoc_server_api_deprecation_errors()
======================================

Synopsis
--------

.. code-block:: c

  void
  mongoc_server_api_deprecation_errors (mongoc_server_api_t *api,
                                        bool deprecation_errors);

Set whether or not to error on commands that are deprecated in this API version.

Parameters
----------

* ``api``: A :symbol:`mongoc_server_api_t`.
* ``deprecation_errors``: Whether or not to error on commands that are deprecated.
