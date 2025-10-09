:man_page: bson_oid_copy_unsafe

bson_oid_copy_unsafe()
======================

Synopsis
--------

.. code-block:: c

  static inline void
  bson_oid_copy_unsafe (const bson_oid_t *src, bson_oid_t *dst);

Description
-----------

Identical to :symbol:`bson_oid_copy()`, but performs no integrity checking.
