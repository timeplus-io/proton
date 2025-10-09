:man_page: mongoc_server_api_version_from_string

mongoc_server_api_version_from_string()
=======================================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_server_api_version_from_string (const char *version,
                                         mongoc_server_api_version_t *out);

Given a string ``version``, populates ``out`` with the equivalent :symbol:`mongoc_server_api_version_t` if ``version`` represents a valid API version.

Parameters
----------

* ``version``: A string representing the version identifier.
* ``out``: A pointer to a :symbol:`mongoc_server_api_version_t`.

Returns
-------

True on success, false on failure. On success, ``out`` is populated with the converted version string. On failure, ``out`` is not altered.
