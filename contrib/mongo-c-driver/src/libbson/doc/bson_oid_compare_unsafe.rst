:man_page: bson_oid_compare_unsafe

bson_oid_compare_unsafe()
=========================

Synopsis
--------

.. code-block:: c

  static inline int
  bson_oid_compare_unsafe (const bson_oid_t *oid1, const bson_oid_t *oid2);

Description
-----------

Identical to :symbol:`bson_oid_compare()`, but performs no integrity checking.
