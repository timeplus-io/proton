:man_page: bson_context_get_default

bson_context_get_default()
==========================

Synopsis
--------

.. code-block:: c

  bson_context_t *
  bson_context_get_default (void);

Returns
-------

The ``bson_context_get_default()`` function shall return the default :symbol:`bson_context_t` for the process.
This context is created with the flags ``BSON_CONTEXT_THREAD_SAFE`` and ``BSON_CONTEXT_DISABLE_PID_CACHE``.