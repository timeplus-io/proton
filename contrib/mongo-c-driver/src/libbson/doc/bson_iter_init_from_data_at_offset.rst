:man_page: bson_iter_init_from_data_at_offset

bson_iter_init_from_data_at_offset()
====================================


Synopsis
--------

.. code-block:: c

   bool
   bson_iter_init_from_data_at_offset (bson_iter_t *iter,
                                       const uint8_t *data,
                                       size_t length,
                                       uint32_t offset,
                                       uint32_t keylen);

Parameters
----------

* ``iter``: A :symbol:`bson_iter_t`.
* ``data``: A buffer to initialize with. This is not validated.
* ``length``: The length of ``data`` in bytes. This is not validated.
* ``offset``: The offset of the field to start iterating. This is not validated. This should be an offset previously obtained from :symbol:`bson_iter_offset()`.
* ``keylen``: The string length of the key of the field to start iterating. This is not validated. This should be a length previously obtained from :symbol:`bson_iter_key_len()`.

Description
-----------

Creates a :symbol:`bson_iter_t` and starts iteration on a field at the offset.

:symbol:`bson_iter_init_from_data_at_offset` is useful for situations where the
progress of a :symbol:`bson_iter_t` must be saved and restored without relying
on the :symbol:`bson_iter_t` data layout. Saving the progress could be
accomplished by:

- Saving the current field's key length with :symbol:`bson_iter_key_len()`
- Saving the current offset with :symbol:`bson_iter_offset()`
- Saving the data pointer of the iterated :symbol:`bson_t` with :symbol:`bson_get_data()`
- Saving the data length of the iterated :symbol:`bson_t` with the ``len`` struct field

Then later, these saved values can be passed to
:symbol:`bson_iter_init_from_data_at_offset()` to reconstruct the
:symbol:`bson_iter_t` in constant time.

Returns
-------

Returns true if the iter was successfully initialized.

.. seealso::

  | :symbol:`bson_iter_key_len()`

  | :symbol:`bson_iter_offset()`

  | :symbol:`bson_get_data()`

