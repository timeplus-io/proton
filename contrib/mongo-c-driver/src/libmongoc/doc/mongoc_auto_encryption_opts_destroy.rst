:man_page: mongoc_auto_encryption_opts_destroy

mongoc_auto_encryption_opts_destroy()
=====================================

Synopsis
--------

.. code-block:: c

   void
   mongoc_auto_encryption_opts_destroy (mongoc_auto_encryption_opts_t *opts);

Destroy a :symbol:`mongoc_auto_encryption_opts_t`.

Parameters
----------

* ``opts`` The :symbol:`mongoc_auto_encryption_opts_t` to destroy.

.. seealso::

  | :symbol:`mongoc_auto_encryption_opts_new()`

  | `In-Use Encryption <in-use-encryption_>`_

