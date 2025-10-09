:man_page: mongoc_write_concern_set_journal

mongoc_write_concern_set_journal()
==================================

Synopsis
--------

.. code-block:: c

  void
  mongoc_write_concern_set_journal (mongoc_write_concern_t *write_concern,
                                    bool journal);

Parameters
----------

* ``write_concern``: A :symbol:`mongoc_write_concern_t`.
* ``journal``: A boolean.

Description
-----------

Sets if the write must have been journaled before indicating success.

Beginning in version 1.9.0, this function can now alter the write concern after
it has been used in an operation. Previously, using the struct with an operation
would mark it as "frozen" and calling this function would log a warning instead
instead of altering the write concern.
