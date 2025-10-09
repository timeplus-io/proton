:man_page: bson_context_t

bson_context_t
==============

BSON OID Generation Context

Synopsis
--------

.. code-block:: c

  #include <bson/bson.h>

  typedef enum {
    BSON_CONTEXT_NONE = 0,
    BSON_CONTEXT_DISABLE_PID_CACHE = (1 << 2),
  } bson_context_flags_t;

  typedef struct _bson_context_t bson_context_t;

  bson_context_t *
  bson_context_get_default (void) BSON_GNUC_CONST;
  bson_context_t *
  bson_context_new (bson_context_flags_t flags);
  void
  bson_context_destroy (bson_context_t *context);

Description
-----------

The :symbol:`bson_context_t` structure is context for generation of BSON Object
IDs. This context allows overriding behavior of generating ObjectIDs. The flags
``BSON_CONTEXT_NONE``, ``BSON_CONTEXT_THREAD_SAFE``, and ``BSON_CONTEXT_DISABLE_PID_CACHE``
are the only ones used. The others have no effect.

.. only:: html

  Functions
  ---------

  .. toctree::
    :titlesonly:
    :maxdepth: 1

    bson_context_destroy
    bson_context_get_default
    bson_context_new

Example
-------

.. code-block:: c

  #include <bson/bson.h>

  int
  main (int argc, char *argv[])
  {
     bson_context_t *ctx = NULL;
     bson_oid_t oid;

     /* use default context, via bson_context_get_default() */
     bson_oid_init (&oid, NULL);

     /* specify a local context for additional control */
     ctx = bson_context_new (BSON_CONTEXT_NONE);
     bson_oid_init (&oid, ctx);

     bson_context_destroy (ctx);

     return 0;
  }

