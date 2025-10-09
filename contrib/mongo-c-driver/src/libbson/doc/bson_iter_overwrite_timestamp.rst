:man_page: bson_iter_overwrite_timestamp

bson_iter_overwrite_timestamp()
===============================

Synopsis
--------

.. code-block:: c

  void
  bson_iter_overwrite_timestamp (bson_iter_t *iter,
                                 uint32_t timestamp,
                                 uint32_t increment);

Parameters
----------

* ``iter``: A :symbol:`bson_iter_t`.
* ``timestamp``: A uint32_t.
* ``increment``: A uint32_t.

Description
-----------

The ``bson_iter_overwrite_timestamp()`` function shall overwrite the contents of a BSON_TYPE_TIMESTAMP element in place.

This may only be done when the underlying bson document allows mutation.

It is a programming error to call this function when ``iter`` is not observing an element of type BSON_TYPE_TIMESTAMP.

