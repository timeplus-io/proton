:man_page: mongoc_apm_set_topology_closed_cb

mongoc_apm_set_topology_closed_cb()
===================================

Synopsis
--------

.. code-block:: c

  typedef void (*mongoc_apm_topology_closed_cb_t) (
     const mongoc_apm_topology_closed_t *event);

  void
  mongoc_apm_set_topology_closed_cb (mongoc_apm_callbacks_t *callbacks,
                                     mongoc_apm_topology_closed_cb_t cb);

Receive an event notification whenever the driver stops monitoring a server topology and destroys its :symbol:`mongoc_topology_description_t`.

Parameters
----------

* ``callbacks``: A :symbol:`mongoc_apm_callbacks_t`.
* ``cb``: A function to call with a :symbol:`mongoc_apm_topology_closed_t` whenever the driver stops monitoring a server topology and destroys its :symbol:`mongoc_topology_description_t`.

.. seealso::

  | :doc:`Introduction to Application Performance Monitoring <application-performance-monitoring>`

