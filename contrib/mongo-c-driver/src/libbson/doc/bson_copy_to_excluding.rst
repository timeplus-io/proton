:man_page: bson_copy_to_excluding

bson_copy_to_excluding()
========================

.. warning::
   .. deprecated:: 1.1.0

      This function is deprecated and should not be used in new code.

      Please use :symbol:`bson_copy_to_excluding_noinit()` in new code.

Synopsis
--------

.. code-block:: c

  void
  bson_copy_to_excluding (const bson_t *src,
                          bson_t *dst,
                          const char *first_exclude,
                          ...) BSON_GNUC_NULL_TERMINATED
     BSON_GNUC_DEPRECATED_FOR (bson_copy_to_excluding_noinit);

Parameters
----------

* ``src``: A :symbol:`bson_t`.
* ``dst``: A :symbol:`bson_t`.
* ``first_exclude``: The first field name to exclude.

Description
-----------

The :symbol:`bson_copy_to_excluding()` function shall copy all fields from
``src`` to ``dst`` except those specified by the variadic, NULL terminated list
of keys starting from ``first_exclude``.

.. warning::

  :symbol:`bson_init` is called on ``dst``.

