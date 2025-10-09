:man_page: mongoc_apm_set_topology_changed_cb

mongoc_apm_set_topology_changed_cb()
====================================

Synopsis
--------

.. code-block:: c

  typedef void (*mongoc_apm_topology_changed_cb_t) (
     const mongoc_apm_topology_changed_t *event);

  void
  mongoc_apm_set_topology_changed_cb (mongoc_apm_callbacks_t *callbacks,
                                      mongoc_apm_topology_changed_cb_t cb);

Receive an event notification whenever the driver observes a change in any of the servers it is connected to or a change in the overall server topology.

Parameters
----------

* ``callbacks``: A :symbol:`mongoc_apm_callbacks_t`.
* ``cb``: A function to call with a :symbol:`mongoc_apm_topology_changed_t` whenever the driver observes a change in any of the servers it is connected to or a change in the overall server topology.

.. seealso::

  | :doc:`Introduction to Application Performance Monitoring <application-performance-monitoring>`

