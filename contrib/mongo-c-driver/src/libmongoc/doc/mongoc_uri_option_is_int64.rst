:man_page: mongoc_uri_option_is_int64

mongoc_uri_option_is_int64()
============================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_uri_option_is_int64 (const char *option);

Parameters
----------

* ``option``: The name of an option, case insensitive.

Description
-----------

Returns true if the option is a known MongoDB URI option of 64-bit integer type. For example, "wTimeoutMS=100" is a valid 64-bit integer MongoDB URI option, so ``mongoc_uri_option_is_int64 ("wTimeoutMS")`` is true.

.. seealso::

  | :symbol:`mongoc_uri_option_is_int32()`

