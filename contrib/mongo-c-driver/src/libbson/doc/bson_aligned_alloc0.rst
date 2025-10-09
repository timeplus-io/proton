:man_page: bson_aligned_alloc0

bson_aligned_alloc0()
=====================

Synopsis
--------

.. code-block:: c

  void *
  bson_aligned_alloc0 (size_t alignment, size_t num_bytes);

Parameters
----------

* ``alignment``: The alignment of the allocated bytes of memory. Must be a power of 2 and a multiple of ``sizeof (void *)``.
* ``num_bytes``: The number of bytes to allocate. Must be a multiple of ``alignment``.

Description
-----------

This is a portable ``aligned_alloc()`` wrapper that also sets the memory to zero.

In general, this function will return an allocation at least ``sizeof(void*)`` bytes or bigger with an alignment of at least ``alignment``.

If there was a failure to allocate ``num_bytes`` bytes aligned to ``alignment``, the process will be aborted.

.. warning::

  This function will abort on failure to allocate memory.

Returns
-------

A pointer to a memory region which *HAS* been zeroed.
