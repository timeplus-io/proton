:man_page: bson_array_as_canonical_extended_json

bson_array_as_canonical_extended_json()
=======================================

Synopsis
--------

.. code-block:: c

  char *
  bson_array_as_canonical_extended_json (const bson_t *bson, size_t *length);

Parameters
----------

* ``bson``: A :symbol:`bson_t`.
* ``length``: An optional location for the length of the resulting string.

Description
-----------

The :symbol:`bson_array_as_canonical_extended_json()` encodes ``bson`` as a UTF-8 string in the canonical `MongoDB Extended JSON format`_, except the outermost element is encoded as a JSON array, rather than a JSON document.

The caller is responsible for freeing the resulting UTF-8 encoded string by calling :symbol:`bson_free()` with the result.

If non-NULL, ``length`` will be set to the length of the result in bytes.

Returns
-------

If successful, a newly allocated UTF-8 encoded string and ``length`` is set.

Upon failure, NULL is returned.

Example
-------

.. code-block:: c

  #include <bson/bson.h>

  int main ()
  {
     bson_t bson;
     char *str;

     bson_init (&bson);
     /* BSON array is a normal BSON document with integer values for the keys,
      * starting with 0 and continuing sequentially
      */
     BSON_APPEND_INT32 (&bson, "0", 1);
     BSON_APPEND_UTF8 (&bson, "1", "bar");

     str = bson_array_as_canonical_extended_json (&bson, NULL);
     /* Prints
      * [ { "$numberInt" : 1 }, "bar" ]
      */
     printf ("%s\n", str);
     bson_free (str);

     bson_destroy (&bson);
  }


.. only:: html

  .. include:: includes/seealso/bson-as-json.txt

.. _MongoDB Extended JSON format: https://github.com/mongodb/specifications/blob/master/source/extended-json.rst
