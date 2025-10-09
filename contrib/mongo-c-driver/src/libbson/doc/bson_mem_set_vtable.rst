:man_page: bson_mem_set_vtable

bson_mem_set_vtable()
=====================

Synopsis
--------

.. code-block:: c

  typedef struct _bson_mem_vtable_t {
     void *(*malloc) (size_t num_bytes);
     void *(*calloc) (size_t n_members, size_t num_bytes);
     void *(*realloc) (void *mem, size_t num_bytes);
     void (*free) (void *mem);
     void *(*aligned_alloc) (size_t alignment, size_t num_bytes);
     void *padding[3];
  } bson_mem_vtable_t;

  void
  bson_mem_set_vtable (const bson_mem_vtable_t *vtable);

Parameters
----------

* ``vtable``: A bson_mem_vtable_t with every non-padding field set.

Description
-----------

This function shall install a new memory allocator to be used by Libbson.

For backwards compatibility, if ``vtable->aligned_alloc`` is not set, calls to ``vtable->aligned_alloc`` will use ``vtable->malloc`` instead.

.. warning::

  This function *MUST* be called at the beginning of the process. Failure to do so will result in memory being freed by the wrong allocator.
