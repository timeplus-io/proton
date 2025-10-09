:man_page: bson_oid_get_time_t_unsafe

bson_oid_get_time_t_unsafe()
============================

Synopsis
--------

.. code-block:: c

  static inline time_t
  bson_oid_get_time_t_unsafe (const bson_oid_t *oid);

Description
-----------

Identical to :symbol:`bson_oid_get_time_t()`, but performs no integrity checking.
