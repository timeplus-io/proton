:man_page: mongoc_find_and_modify_opts_set_max_time_ms

mongoc_find_and_modify_opts_set_max_time_ms()
=============================================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_find_and_modify_opts_set_max_time_ms (
     mongoc_find_and_modify_opts_t *opts, uint32_t max_time_ms);

Parameters
----------

* ``opts``: A :symbol:`mongoc_find_and_modify_opts_t`.
* ``max_time_ms``: The maximum server-side execution time permitted, in milliseconds, or 0 to specify no maximum time (the default setting).

Description
-----------

Adds a maxTimeMS argument to the builder.

Returns
-------

Returns ``true`` if it successfully added the option to the builder, otherwise ``false`` and logs an error.

Note: although ``max_time_ms`` is a uint32_t, it is possible to set it as a uint64_t through the options arguments in some cursor returning functions like :symbol:`mongoc_collection_find_with_opts()`.

Setting maxTimeMS
-----------------

.. literalinclude:: ../examples/find_and_modify_with_opts/fam.c
   :language: c
   :start-after: /* EXAMPLE_FAM_OPTS_BEGIN */
   :end-before: /* EXAMPLE_FAM_OPTS_END */
   :caption: opts.c

