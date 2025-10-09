:man_page: bson_oid_init_from_string_unsafe

bson_oid_init_from_string_unsafe()
==================================

Synopsis
--------

.. code-block:: c

  static inline void
  bson_oid_init_from_string_unsafe (bson_oid_t *oid, const char *str);

Description
-----------

Identical to :symbol:`bson_oid_init_from_string()`, but performs no integrity checking.
