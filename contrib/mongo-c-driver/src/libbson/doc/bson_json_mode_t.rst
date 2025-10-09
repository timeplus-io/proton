:man_page: bson_json_mode_t

bson_json_mode_t
================

BSON JSON encoding mode enumeration

Synopsis
--------

.. code-block:: c

  #include <bson/bson.h>

  typedef enum {
     BSON_JSON_MODE_LEGACY,
     BSON_JSON_MODE_CANONICAL,
     BSON_JSON_MODE_RELAXED,
  } bson_json_mode_t;

Description
-----------

The :symbol:`bson_json_mode_t` enumeration contains all available modes for encoding BSON into `MongoDB Extended JSON`_.

.. seealso::

  | :symbol:`bson_as_json_with_opts()`

.. _MongoDB Extended JSON: https://github.com/mongodb/specifications/blob/master/source/extended-json.rst
