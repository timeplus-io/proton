:man_page: mongoc_write_concern_get_fsync

mongoc_write_concern_get_fsync()
================================

.. warning::
   .. deprecated:: 1.4.0

      This function is deprecated and should not be used in new code.

      Please use :symbol:`mongoc_write_concern_get_journal()` in new code.

Synopsis
--------

.. code-block:: c

  bool
  mongoc_write_concern_get_fsync (const mongoc_write_concern_t *write_concern);

Parameters
----------

* ``write_concern``: A :symbol:`mongoc_write_concern_t`.

Description
-----------

Fetches if an fsync should be performed before returning success on a write operation.

Returns
-------

Returns true if ``fsync`` is set as part of the write concern.

