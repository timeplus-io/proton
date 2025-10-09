:man_page: mongoc_uri_get_option_as_int64

mongoc_uri_get_option_as_int64()
================================

Synopsis
--------

.. code-block:: c

  int64_t
  mongoc_uri_get_option_as_int64 (const mongoc_uri_t *uri,
                                  const char *option,
                                  int64_t fallback);

Parameters
----------

* ``uri``: A :symbol:`mongoc_uri_t`.
* ``option``: The name of an option, case insensitive.
* ``fallback``: A default value to return.

Description
-----------

Returns the value of the URI option if it is set and of the correct type (integer). Returns ``fallback`` if the option is not set, set to an invalid type, or zero.

Zero is considered "unset", so URIs can be constructed like so, and still accept default values:

.. code-block:: c

  bson_strdup_printf ("mongodb://localhost/?wTimeoutMS=%" PRId64, myvalue)

If ``myvalue`` is non-zero it is the write concern timeout; if it is zero the driver uses the default timeout.

.. seealso::

  | :symbol:`mongoc_uri_get_option_as_int32()`

