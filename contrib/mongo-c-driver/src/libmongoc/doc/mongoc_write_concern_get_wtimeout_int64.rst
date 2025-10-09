:man_page: mongoc_write_concern_get_wtimeout_int64

mongoc_write_concern_get_wtimeout_int64()
=========================================

Synopsis
--------

.. code-block:: c

  int64_t
  mongoc_write_concern_get_wtimeout_int64 (const mongoc_write_concern_t *write_concern);

Parameters
----------

* ``write_concern``: A :symbol:`mongoc_write_concern_t`.

Description
-----------

Get the timeout in milliseconds that the server should wait before returning with a write concern timeout.

A value of 0 indicates no write timeout.

Returns
-------

Returns a 64-bit signed integer containing the timeout.

.. seealso::

  | :symbol:`mongoc_write_concern_set_wtimeout_int64`.

