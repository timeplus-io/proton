:man_page: mongoc_read_concern_set_level

mongoc_read_concern_set_level()
===============================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_read_concern_set_level (mongoc_read_concern_t *read_concern,
                                 const char *level);

Parameters
----------

* ``read_concern``: A :symbol:`mongoc_read_concern_t`.
* ``level``: The readConcern level to use.

Description
-----------

Sets the read concern level. See :symbol:`mongoc_read_concern_t` for details.

Beginning in version 1.9.0, this function can now alter the read concern after
it has been used in an operation. Previously, using the struct with an operation
would mark it as "frozen" and calling this function would return ``false``
instead of altering the read concern.

Returns
-------

Returns ``true`` if the read concern level was set, or ``false`` otherwise.
