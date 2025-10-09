:man_page: bson_reinit

bson_reinit()
=============

Synopsis
--------

.. code-block:: c

  void
  bson_reinit (bson_t *b);

Parameters
----------

* ``b``: A :symbol:`bson_t`.

Description
-----------

Reinitializes a :symbol:`bson_t`.

If the :symbol:`bson_t` structure contains a malloc()'d buffer, it may be reused. To be certain that any buffer is freed, always call :symbol:`bson_destroy` on any :symbol:`bson_t` structure, whether initialized or reinitialized, after its final use.

.. only:: html

  .. include:: includes/seealso/create-bson.txt
