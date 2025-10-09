:man_page: mongoc_cursor_is_alive

mongoc_cursor_is_alive()
========================

.. warning::
   .. deprecated:: 1.10.0

      This function is deprecated and should not be used in new code.

      Please use :symbol:`mongoc_cursor_more()` in new code.


Synopsis
--------

.. code-block:: c

  bool
  mongoc_cursor_is_alive (const mongoc_cursor_t *cursor)
     BSON_GNUC_DEPRECATED_FOR (mongoc_cursor_more);

Parameters
----------

* ``cursor``: A :symbol:`mongoc_cursor_t`.


Returns
-------

See :symbol:`mongoc_cursor_more()`.
