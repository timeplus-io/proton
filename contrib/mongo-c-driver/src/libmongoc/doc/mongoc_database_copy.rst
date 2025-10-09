:man_page: mongoc_database_copy

mongoc_database_copy()
======================

Synopsis
--------

.. code-block:: c

  mongoc_database_t *
  mongoc_database_copy (mongoc_database_t *database) BSON_GNUC_WARN_UNUSED_RESULT;

Parameters
----------

* ``database``: A :symbol:`mongoc_database_t`.

Description
-----------

Performs a deep copy of the ``database`` struct and its configuration. Useful if you intend to call :symbol:`mongoc_database_set_write_concern`, :symbol:`mongoc_database_set_read_prefs`, or :symbol:`mongoc_database_set_read_concern`, and want to preserve an unaltered copy of the struct.

Returns
-------

A newly allocated :symbol:`mongoc_database_t` that should be freed with :symbol:`mongoc_database_destroy()` when no longer in use.

