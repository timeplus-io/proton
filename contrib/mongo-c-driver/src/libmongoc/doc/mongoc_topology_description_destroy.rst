:man_page: mongoc_topology_description_destroy

mongoc_topology_description_destroy()
=====================================

Synopsis
--------

.. code-block:: c

  void
  mongoc_topology_description_destroy (mongoc_topology_description_t *description);

Parameters
----------

* ``description``: A :symbol:`mongoc_topology_description_t`.

Description
-----------

Frees all resources associated with the topology description. Does nothing if ``description`` is NULL.
