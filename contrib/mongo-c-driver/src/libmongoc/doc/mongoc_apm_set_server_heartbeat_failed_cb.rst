:man_page: mongoc_apm_set_server_heartbeat_failed_cb

mongoc_apm_set_server_heartbeat_failed_cb()
===========================================

Synopsis
--------

.. code-block:: c

  typedef void (*mongoc_apm_server_heartbeat_failed_cb_t) (
     const mongoc_apm_server_heartbeat_failed_t *event);

  void
  mongoc_apm_set_server_heartbeat_failed_cb (mongoc_apm_callbacks_t *callbacks,
                                             mongoc_apm_server_heartbeat_failed_cb_t cb);

Receive an event notification whenever the driver fails to send a "hello" command to check the status of a server.

Parameters
----------

* ``callbacks``: A :symbol:`mongoc_apm_callbacks_t`.
* ``cb``: A function to call with a :symbol:`mongoc_apm_server_heartbeat_failed_t` whenever the driver fails to send a "hello" command to check the status of a server.

.. seealso::

  | :doc:`Introduction to Application Performance Monitoring <application-performance-monitoring>`

