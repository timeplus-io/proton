:man_page: mongoc_apm_server_heartbeat_started_get_awaited

mongoc_apm_server_heartbeat_started_get_awaited()
=================================================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_apm_server_heartbeat_started_get_awaited (
     const mongoc_apm_server_heartbeat_started_t *event);

Returns whether this event came from an awaitable hello.

Parameters
----------

* ``event``: A :symbol:`mongoc_apm_server_heartbeat_started_t`.

Returns
-------

A bool indicating whether the heartbeat event came from an awaitable hello.

.. seealso::

  | :doc:`Introduction to Application Performance Monitoring <application-performance-monitoring>`

