:man_page: mongoc_server_api_get_deprecation_errors

mongoc_server_api_get_deprecation_errors()
==========================================

Synopsis
--------

.. code-block:: c

  const mongoc_optional_t *
  mongoc_server_api_get_deprecation_errors (const mongoc_server_api_t *api);

Returns the value of the deprecation_errors flag for the :symbol:`mongoc_server_api_t`.

Parameters
----------

* ``api``: A :symbol:`mongoc_server_api_t`.

Returns
-------

Returns a :symbol:`mongoc_optional_t` indicating whether the deprecation_errors flag was set.
