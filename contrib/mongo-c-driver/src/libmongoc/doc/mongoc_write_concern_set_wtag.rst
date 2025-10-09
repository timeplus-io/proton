:man_page: mongoc_write_concern_set_wtag

mongoc_write_concern_set_wtag()
===============================

Synopsis
--------

.. code-block:: c

  void
  mongoc_write_concern_set_wtag (mongoc_write_concern_t *write_concern,
                                 const char *tag);

Parameters
----------

* ``write_concern``: A :symbol:`mongoc_write_concern_t`.
* ``tag``: A string containing the write tag.

Description
-----------

Sets the write tag that must be satisfied for the write to indicate success. Write tags are preset write concerns configured on your MongoDB server. See :symbol:`mongoc_write_concern_t` for more information on this setting.

Beginning in version 1.9.0, this function can now alter the write concern after
it has been used in an operation. Previously, using the struct with an operation
would mark it as "frozen" and calling this function would log a warning instead
instead of altering the write concern.
