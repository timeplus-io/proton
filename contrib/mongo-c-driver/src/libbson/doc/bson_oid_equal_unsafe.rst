:man_page: bson_oid_equal_unsafe

bson_oid_equal_unsafe()
=======================

Synopsis
--------

.. code-block:: c

  static inline bool
  bson_oid_equal_unsafe (const bson_oid_t *oid1, const bson_oid_t *oid2);

Description
-----------

Identical to :symbol:`bson_oid_equal()`, but performs no integrity checking.
