:man_page: bson_json_opts_t

bson_json_opts_t
================

BSON to JSON encoding options

Synopsis
--------

.. code-block:: c

  #include <bson/bson.h>

  typedef struct _bson_json_opts_t bson_json_opts_t;

  bson_json_opts_t *
  bson_json_opts_new (bson_json_mode_t mode, int32_t max_len);

  void
  bson_json_opts_destroy (bson_json_opts_t *opts);


Description
-----------

The :symbol:`bson_json_opts_t` structure contains options for encoding BSON into `MongoDB Extended JSON`_.

The ``mode`` member is a :symbol:`bson_json_mode_t` defining the encoding mode.

The ``max_len`` member holds a maximum length for the resulting JSON string. Encoding will stop once the serialised string has reached this length. To encode the full BSON document, ``BSON_MAX_LEN_UNLIMITED`` can be used.

.. seealso::

  | :symbol:`bson_as_json_with_opts()`

.. _MongoDB Extended JSON: https://github.com/mongodb/specifications/blob/master/source/extended-json.rst


.. only:: html

	  Functions
	  ---------

	  .. toctree::
	     :titlesonly:
	     :maxdepth: 1

	     bson_json_opts_new
	     bson_json_opts_destroy
	     bson_json_opts_set_outermost_array
