:man_page: bson_iter_find_w_len

bson_iter_find_w_len()
======================

Synopsis
--------

.. code-block:: c

  bool
  bson_iter_find_w_len (bson_iter_t *iter, const char *key, int keylen);

Parameters
----------

* ``iter``: A :symbol:`bson_iter_t`.
* ``key``: A string containing the requested key.
* ``keylen``: An integer indicating the length of the key string.

Description
-----------

The ``bson_iter_find_w_len()`` function shall advance ``iter`` to the first element named ``key`` or exhaust all elements of ``iter``. If ``iter`` is exhausted, false is returned and ``iter`` should be considered invalid.

``key`` is case-sensitive. For a case-folded version, see :symbol:`bson_iter_find_case()`.

Returns
-------

true is returned if the requested key was found. If not, ``iter`` was exhausted and should now be considered invalid.

