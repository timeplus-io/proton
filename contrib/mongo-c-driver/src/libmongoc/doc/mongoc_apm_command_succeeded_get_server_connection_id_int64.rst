:man_page: mongoc_apm_command_succeeded_get_server_connection_id_int64

mongoc_apm_command_succeeded_get_server_connection_id_int64()
=============================================================

Synopsis
--------

.. code-block:: c

  int64_t
  mongoc_apm_command_succeeded_get_server_connection_id_int64 (
    const mongoc_apm_command_succeeded_t *event);

Returns the server connection ID for the command. The server connection ID is
distinct from the server ID
(:symbol:`mongoc_apm_command_succeeded_get_server_id`) and is returned by the
hello or legacy hello response as "connectionId" from the server on 4.2+.

Parameters
----------

* ``event``: A :symbol:`mongoc_apm_command_succeeded_t`.

Returns
-------

The server connection ID as a positive integer or -1 if it is not available.

.. seealso::

  | :doc:`Introduction to Application Performance Monitoring <application-performance-monitoring>`

