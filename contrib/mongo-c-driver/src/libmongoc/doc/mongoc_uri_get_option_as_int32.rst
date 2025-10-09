:man_page: mongoc_uri_get_option_as_int32

mongoc_uri_get_option_as_int32()
================================

Synopsis
--------

.. code-block:: c

  int32_t
  mongoc_uri_get_option_as_int32 (const mongoc_uri_t *uri,
                                  const char *option,
                                  int32_t fallback);

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

  bson_strdup_printf ("mongodb://localhost/?connectTimeoutMS=%d", myvalue)

If ``myvalue`` is non-zero it is the connection timeout; if it is zero the driver uses the default timeout.

When reading an option that is an int64, this function will return the value as ``int32_t``. If the value is outside the range of a 32-bit integer, a warning will be emitted and ``fallback`` is returned instead.

.. seealso::

  | :symbol:`mongoc_uri_get_option_as_int64()`

