:man_page: bson_iter_key_len

bson_iter_key_len()
===================

Synopsis
--------

.. code-block:: c

  uint32_t
  bson_iter_key_len (const bson_iter_t *iter);

Parameters
----------

* ``iter``: A :symbol:`bson_iter_t`.

Description
-----------

Fetches the length of the key for the current element observed by ``iter``. This is a constant time computation, and therefore faster than calling ``strlen()`` on a key returned by :symbol:`bson_iter_key()`.

Returns
-------

An integer representing the key length.

.. seealso::

  | :symbol:`bson_iter_key()` to retrieve current key.

