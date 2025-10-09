:man_page: mongoc_server_description_last_update_time

mongoc_server_description_last_update_time()
============================================

Synopsis
--------

.. code-block:: c

  int64_t
  mongoc_server_description_last_update_time (const mongoc_server_description_t *description);

Parameters
----------

* ``description``: A :symbol:`mongoc_server_description_t`.

Description
-----------

Get the last point in time when we processed a hello for this server, or, if we have not processed any hellos since creating the description, the time the server description was initialized.

Returns
-------

The server's last update time, in microseconds.
