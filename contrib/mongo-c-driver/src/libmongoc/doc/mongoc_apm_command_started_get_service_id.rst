:man_page: mongoc_apm_command_started_get_service_id

mongoc_apm_command_started_get_service_id()
===========================================

Synopsis
--------

.. code-block:: c

  const bson_oid_t *
  mongoc_apm_command_started_get_service_id (
     const mongoc_apm_command_started_t *event);

Returns this event's service ID, which identifies the MongoDB service behind a
load balancer, or ``NULL`` if not connected to a load balanced cluster.

Parameters
----------

* ``event``: A :symbol:`mongoc_apm_command_started_t`.

Returns
-------

A :symbol:`bson_oid_t` that should not be modified or freed or ``NULL``.
