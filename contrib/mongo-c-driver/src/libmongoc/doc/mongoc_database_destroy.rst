:man_page: mongoc_database_destroy

mongoc_database_destroy()
=========================

Synopsis
--------

.. code-block:: c

  void
  mongoc_database_destroy (mongoc_database_t *database);

Releases all resources associated with ``database``, including freeing the structure. Does nothing if ``database`` is NULL.

Parameters
----------

* ``database``: A :symbol:`mongoc_database_t`.

