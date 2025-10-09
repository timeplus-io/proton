:man_page: bson_init_from_json

bson_init_from_json()
=====================

Synopsis
--------

.. code-block:: c

  bool
  bson_init_from_json (bson_t *bson,
                       const char *data,
                       ssize_t len,
                       bson_error_t *error);

Parameters
----------

* ``bson``: Pointer to an uninitialized :symbol:`bson_t`.
* ``data``: A UTF-8 encoded string containing valid JSON.
* ``len``: The length of ``data`` in bytes excluding a trailing ``\0`` or -1 to determine the length with ``strlen()``.
* ``error``: An optional location for a :symbol:`bson_error_t`.

Description
-----------

The ``bson_init_from_json()`` function will initialize a new :symbol:`bson_t` by parsing the JSON found in ``data``. Only a single JSON object may exist in ``data`` or an error will be set and false returned.

``data`` should be in `MongoDB Extended JSON <https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/>`_ format.


Deprecated behavior
-------------------

If there are multiple comma-separated JSONs in ``data``, the keys from all JSONs are merged in the returned BSON.
For example, ``{"a": 1},{"b": 2}`` is parsed as ``{"a": 1, "b": 2}``.

If the first character encountered after the last valid
JSON object is ``{``, all following characters are ignored and no error is set.
Otherwise, an error will be set and NULL returned.

This deprecated behavior is subject to change in a future release.

Errors
------

Errors are propagated via the ``error`` parameter.

Returns
-------

Returns ``true`` if valid JSON was parsed, otherwise ``false`` and ``error`` is set. On success, ``bson`` is initialized and must be freed with :symbol:`bson_destroy`, otherwise ``bson`` is invalid.

.. only:: html

  .. include:: includes/seealso/create-bson.txt
  .. include:: includes/seealso/json.txt
