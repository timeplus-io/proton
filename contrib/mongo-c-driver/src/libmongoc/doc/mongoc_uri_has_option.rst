:man_page: mongoc_uri_has_option

mongoc_uri_has_option()
=======================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_uri_has_option (const mongoc_uri_t *uri, const char *option);

Parameters
----------

* ``uri``: A :symbol:`mongoc_uri_t`.
* ``option``: The name of an option, case insensitive.

Description
-----------

Returns true if the option was present in the initial MongoDB URI.

