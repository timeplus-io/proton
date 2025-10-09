:man_page: bson_json_opts_new

bson_json_opts_new()
====================

Synopsis
--------

.. code-block:: c

  bson_json_opts_t *
  bson_json_opts_new (bson_json_mode_t mode, int32_t max_len);

Parameters
----------

* ``mode``: A bson_json_mode_t.
* ``max_len``: An int32_t.

Description
-----------

The :symbol:`bson_json_opts_new()` function shall create a new :symbol:`bson_json_opts_t` using the mode and length supplied.  The ``mode`` member is a :symbol:`bson_json_mode_t` defining the encoding mode.

The ``max_len`` member holds a maximum length for the resulting JSON string. Encoding will stop once the serialised string has reached this length. To encode the full BSON document, ``BSON_MAX_LEN_UNLIMITED`` can be used.

Returns
-------

A newly allocated :symbol:`bson_json_opts_t`.

