:man_page: bson_append_regex_w_len

bson_append_regex_w_len()
==========================

Synopsis
--------

.. code-block:: c

  bool
  bson_append_regex_w_len (bson_t *bson,
                            const char *key,
                            int key_length,
                            const char *regex,
                            int regex_length,
                            const char *options);

Parameters
----------

* ``bson``: A :symbol:`bson_t`.
* ``key``: An ASCII C string containing the name of the field.
* ``key_length``: The length of ``key`` in bytes, or -1 to determine the length with ``strlen()``.
* ``regex``: An ASCII string containing the regex.
* ``regex_length``: The length of ``regex`` in bytes, or -1 to determine the length with ``strlen()``.
* ``options``: An optional string containing the regex options as a string.

Description
-----------

Appends a new field to ``bson`` of type BSON_TYPE_REGEX. ``regex`` should be the regex string. ``options`` should contain the options for the regex.

Valid characters for ``options`` include:

* ``'i'`` for case-insensitive.
* ``'m'`` for multiple matching.
* ``'x'`` for verbose mode.
* ``'l'`` to make \w and \W locale dependent.
* ``'s'`` for dotall mode ('.' matches everything)
* ``'u'`` to make \w and \W match unicode.

Returns
-------

Returns ``true`` if the operation was applied successfully. The function will fail if appending the regex grows ``bson`` larger than INT32_MAX.

