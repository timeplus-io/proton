:man_page: mongoc_write_concern_set_wtimeout_int64

mongoc_write_concern_set_wtimeout_int64()
=========================================

Synopsis
--------

.. code-block:: c

  void
  mongoc_write_concern_set_wtimeout_int64 (mongoc_write_concern_t *write_concern,
                                           int64_t wtimeout_msec);

Parameters
----------

* ``write_concern``: A :symbol:`mongoc_write_concern_t`.
* ``wtimeout_msec``: A positive ``int64_t`` or zero.

Description
-----------

Set the timeout in milliseconds that the server should wait before returning with a write concern timeout. This is not the same as a socket timeout. A value of zero may be used to indicate no write concern timeout.

Beginning in version 1.9.0, this function can now alter the write concern after
it has been used in an operation. Previously, using the struct with an operation
would mark it as "frozen" and calling this function would log a warning instead
instead of altering the write concern.

.. seealso::

  | :symbol:`mongoc_write_concern_get_wtimeout_int64`.

