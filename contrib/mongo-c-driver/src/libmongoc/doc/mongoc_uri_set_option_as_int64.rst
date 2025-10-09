:man_page: mongoc_uri_set_option_as_int64

mongoc_uri_set_option_as_int64()
================================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_uri_set_option_as_int64 (const mongoc_uri_t *uri,
                                  const char *option,
                                  int64_t value);

Parameters
----------

* ``uri``: A :symbol:`mongoc_uri_t`.
* ``option``: The name of an option, case insensitive.
* ``value``: The new value.

Description
-----------

Sets an individual URI option, after the URI has been parsed from a string.

Only known options of type int32 or int64 can be set. For 32-bit integer options, the function returns ``false`` when trying to set a 64-bit value that exceeds the range of an ``int32_t``. Values that fit into an ``int32_t`` will be set correctly. In both cases, a warning will be emitted.

Updates the option in-place if already set, otherwise appends it to the URI's :symbol:`bson:bson_t` of options.

Returns
-------

True if successfully set (the named option is a known option of type int64).

.. seealso::

  | :symbol:`mongoc_uri_option_is_int64()`

  | :symbol:`mongoc_uri_set_option_as_int32()`

