:man_page: mongoc_client_set_apm_callbacks

mongoc_client_set_apm_callbacks()
=================================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_client_set_apm_callbacks (mongoc_client_t *client,
                                   mongoc_apm_callbacks_t *callbacks,
                                   void *context);

Register a set of callbacks to receive Application Performance Monitoring events.

The ``callbacks`` are copied by the client and may be destroyed at any time after. If a ``context`` is passed, it is the application's responsibility to ensure ``context`` remains valid for the lifetime of the client.

Parameters
----------

* ``client``: A :symbol:`mongoc_client_t`.
* ``callbacks``: Optional :symbol:`mongoc_apm_callbacks_t`. Pass NULL to clear all callbacks.
* ``context``: Optional pointer to include with each event notification.

Returns
-------

Returns true on success. If any arguments are invalid, returns false and logs an error.

.. seealso::

  | :doc:`Introduction to Application Performance Monitoring <application-performance-monitoring>`

