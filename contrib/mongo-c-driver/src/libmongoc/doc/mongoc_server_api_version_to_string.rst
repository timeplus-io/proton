:man_page: mongoc_server_api_version_to_string

mongoc_server_api_version_to_string()
=====================================

Synopsis
--------

.. code-block:: c

  const char *
  mongoc_server_api_version_to_string (mongoc_server_api_version_t version);

Returns the string representation of ``version``, or NULL if ``version`` has no string representation.

Parameters
----------

* ``version``: A :symbol:`mongoc_server_api_version_t`

Returns
-------

Returns the string representation of ``version`` or NULL.  The returned string should not be freed.
