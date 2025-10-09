:man_page: bson_oid_hash_unsafe

bson_oid_hash_unsafe()
======================

Synopsis
--------

.. code-block:: c

  static inline uint32_t
  bson_oid_hash_unsafe (const bson_oid_t *oid);

Description
-----------

Identical to :symbol:`bson_oid_hash()`, but performs no integrity checking.
