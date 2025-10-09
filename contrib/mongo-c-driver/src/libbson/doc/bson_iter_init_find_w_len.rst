:man_page: bson_iter_init_find_w_len

bson_iter_init_find_w_len()
===========================

Synopsis
--------

.. code-block:: c

  bool
  bson_iter_init_find_w_len (bson_iter_t *iter,
                             const bson_t *bson,
                             const char *key,
                             int keylen);

Parameters
----------

* ``iter``: A :symbol:`bson_iter_t`.
* ``bson``: A :symbol:`bson_t`.
* ``key``: A key to locate after initializing the iter.
* ``keylen``: An integer indicating the length of the key string.

Description
-----------

This function is identical to ``(bson_iter_init() && bson_iter_find_w_len())``.

.. only:: html

  .. include:: includes/seealso/iter-init.txt

