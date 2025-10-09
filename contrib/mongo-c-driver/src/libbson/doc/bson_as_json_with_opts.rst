:man_page: bson_as_json_with_opts

bson_as_json_with_opts()
========================

Synopsis
--------

.. code-block:: c

  char *
  bson_as_json_with_opts (const bson_t *bson, size_t *length, const bson_json_opts_t *opts);

Parameters
----------

* ``bson``: A :symbol:`bson_t`.
* ``length``: An optional location for the length of the resulting string.
* ``opts``: A :symbol:`bson_json_opts_t`.

Description
-----------

The :symbol:`bson_as_json_with_opts()` encodes ``bson`` as a UTF-8 string in the `MongoDB Extended JSON format`_.

The caller is responsible for freeing the resulting UTF-8 encoded string by calling :symbol:`bson_free()` with the result.

If non-NULL, ``length`` will be set to the length of the result in bytes.

The ``opts`` structure is used to pass options for the encoding process. Please refer to the documentation of :symbol:`bson_json_opts_t` for more details.

Returns
-------

If successful, a newly allocated UTF-8 encoded string and ``length`` is set.

Upon failure, NULL is returned.

Example
-------

.. code-block:: c

  bson_json_opts_t *opts = bson_json_opts_new (BSON_JSON_MODE_CANONICAL, BSON_MAX_LEN_UNLIMITED);
  char *str = bson_as_json_with_opts (doc, NULL, opts);
  printf ("%s\n", str);
  bson_free (str);
  bson_json_opts_destroy (opts);


.. only:: html

  .. include:: includes/seealso/bson-as-json.txt

.. _MongoDB Extended JSON format: https://github.com/mongodb/specifications/blob/master/source/extended-json.rst
