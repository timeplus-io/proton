:man_page: mongoc_server_api_get_strict

mongoc_server_api_get_strict()
==============================

Synopsis
--------

.. code-block:: c

  const mongoc_optional_t *
  mongoc_server_api_get_strict (const mongoc_server_api_t *api);

Returns the value of the strict flag for the :symbol:`mongoc_server_api_t`.

Parameters
----------

* ``api``: A :symbol:`mongoc_server_api_t`.

Returns
-------

Returns a :symbol:`mongoc_optional_t` indicating whether the strict flag was set.
