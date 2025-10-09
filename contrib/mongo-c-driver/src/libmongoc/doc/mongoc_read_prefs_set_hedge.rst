:man_page: mongoc_read_prefs_set_hedge

mongoc_read_prefs_set_hedge()
=============================

Synopsis
--------

.. code-block:: c

  void
  mongoc_read_prefs_set_hedge (mongoc_read_prefs_t *read_prefs,
                               const bson_t *hedge);

Parameters
----------

* ``read_prefs``: A :symbol:`mongoc_read_prefs_t`.
* ``hedge``: A :symbol:`bson:bson_t`.

Description
-----------

Sets the hedge document to be used for the read preference. Sharded clusters running MongoDB 4.4 or later can dispatch read operations in parallel, returning the result from the fastest host and cancelling the unfinished operations.

For example, this is a valid hedge document

.. code-block:: none

   {
      enabled: true
   }

Appropriate values for the ``enabled`` key are ``true`` or ``false``.
