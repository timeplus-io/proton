:man_page: bson_iter_offset

bson_iter_offset()
==================

Synopsis
--------

.. code-block:: c

  uint32_t
  bson_iter_offset (const bson_iter_t *iter);

Parameters
----------

* ``iter``: A :symbol:`bson_iter_t`.

Description
-----------

Fetches the offset for the current element observed by ``iter``.

Returns
-------

An unsigned integer representing the offset in the BSON data of the current element.

.. seealso::

  | :symbol:`bson_iter_init_from_data_at_offset()` to use this offset to reconstruct a :symbol:`bson_iter_t` in constant time.

