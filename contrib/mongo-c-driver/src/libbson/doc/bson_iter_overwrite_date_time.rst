:man_page: bson_iter_overwrite_date_time

bson_iter_overwrite_date_time()
===============================

Synopsis
--------

.. code-block:: c

  void
  bson_iter_overwrite_date_time (bson_iter_t *iter, int64_t value);

Parameters
----------

* ``iter``: A :symbol:`bson_iter_t`.
* ``value``: The date and time as specified in milliseconds since the UNIX epoch.

Description
-----------

The ``bson_iter_overwrite_date_time()`` function shall overwrite the contents of a BSON_TYPE_DATE_TIME element in place.

This may only be done when the underlying bson document allows mutation.

It is a programming error to call this function when ``iter`` is not observing an element of type BSON_TYPE_DATE_TIME.

