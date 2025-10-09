:man_page: bson_json_opts_destroy

bson_json_opts_destroy()
========================

Synopsis
--------

.. code-block:: c

  void
  bson_json_opts_destroy (bson_json_opts_t *opts);

Parameters
----------

* ``opts``: A :symbol:`bson_json_opts_t`.

Description
-----------

Destroys and releases all resources associated with ``opts``. Does nothing if ``opts`` is NULL.
