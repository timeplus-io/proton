:man_page: mongoc_uri_option_is_int32

mongoc_uri_option_is_int32()
============================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_uri_option_is_int32 (const char *option);

Parameters
----------

* ``option``: The name of an option, case insensitive.

Description
-----------

Returns true if the option is a known MongoDB URI option of integer type. For example, "zlibCompressionLevel=5" is a valid integer MongoDB URI option, so ``mongoc_uri_option_is_int32 ("zlibCompressionLevel")`` is true. This will also return true for all 64-bit integer options.

.. seealso::

  | :symbol:`mongoc_uri_option_is_int64()`

