:man_page: mongoc_apm_command_failed_get_reply

mongoc_apm_command_failed_get_reply()
========================================

Synopsis
--------

.. code-block:: c

  const bson_t *
  mongoc_apm_command_failed_get_reply (
     const mongoc_apm_command_failed_t *event);

Returns the server's reply to a command that failed. The reply contains details about why the command failed. If no server reply was received, such as in the event of a network error, then the reply is a valid empty BSON document. The data is only valid in the scope of the callback that receives this event; copy it if it will be accessed after the callback returns.

Parameters
----------

* ``event``: A :symbol:`mongoc_apm_command_failed_t`.

Returns
-------

A :symbol:`bson:bson_t` that should not be modified or freed.

.. seealso::

  | :doc:`Introduction to Application Performance Monitoring <application-performance-monitoring>`

