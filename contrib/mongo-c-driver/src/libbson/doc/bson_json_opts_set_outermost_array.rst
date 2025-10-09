:man_page: bson_json_opts_set_outermost_array

bson_json_opts_set_outermost_array()
====================================

Synopsis
--------

.. code-block:: c

  void 
  bson_json_opts_set_outermost_array (bson_json_opts_t *opts, bool is_outermost_array);

Parameters
----------

* ``opts``: A :symbol:`bson_json_opts_t`.
* ``is_outermost_array``: A value determining what we want to set the is_outermost_array variable to.

Description
-----------

The :symbol:`bson_json_opts_set_outermost_array()` function shall set the ``is_outermost_array`` variable on the :symbol:`bson_json_opts_t` parameter using the boolean provided.
