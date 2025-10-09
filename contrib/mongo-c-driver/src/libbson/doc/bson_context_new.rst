:man_page: bson_context_new

bson_context_new()
==================

Synopsis
--------

.. code-block:: c

  bson_context_t *
  bson_context_new (bson_context_flags_t flags);

Parameters
----------

* ``flags``: A :symbol:`bson_context_flags_t <bson_context_t>`.

The following ``flags`` may be used:
* ``BSON_CONTEXT_NONE`` meaning creating ObjectIDs with this context is not a thread-safe operation.
* ``BSON_CONTEXT_DISABLE_PID_CACHE`` meaning creating ObjectIDs will also check if the process has
changed by calling ``getpid()`` on every ObjectID generation.

The following flags are deprecated and have no effect:

- ``BSON_CONTEXT_DISABLE_HOST_CACHE``
- ``BSON_CONTEXT_THREAD_SAFE``
- ``BSON_CONTEXT_USE_TASK_ID``

Description
-----------

Creates a new :symbol:`bson_context_t`. This is rarely needed as :symbol:`bson_context_get_default()` serves most use-cases.

Returns
-------

A newly allocated :symbol:`bson_context_t` that should be freed with :symbol:`bson_context_destroy`.

