:man_page: mongoc_topology_description_new_copy

mongoc_topology_description_new_copy()
======================================

Synopsis
--------

.. code-block:: c

  mongoc_topology_description_t *
  mongoc_topology_description_new_copy (
     const mongoc_topology_description_t *description)
     BSON_GNUC_WARN_UNUSED_RESULT;

Parameters
----------

* ``description``: A :symbol:`mongoc_topology_description_t`.

Description
-----------

Performs a deep copy of ``description``.

Returns
-------

Returns a newly allocated copy of ``description`` that should be freed with :symbol:`mongoc_topology_description_destroy()` when no longer in use. Returns NULL if ``description`` is NULL.
