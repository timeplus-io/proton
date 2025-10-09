:man_page: mongoc_apm_set_server_opening_cb

mongoc_apm_set_server_opening_cb()
==================================

Synopsis
--------

.. code-block:: c

  typedef void (*mongoc_apm_server_opening_cb_t) (
     const mongoc_apm_server_opening_t *event);

  void
  mongoc_apm_set_server_opening_cb (mongoc_apm_callbacks_t *callbacks,
                                    mongoc_apm_server_opening_cb_t cb);

Receive an event notification whenever the driver adds a :symbol:`mongoc_server_description_t` for a new server it was not monitoring before.

Parameters
----------

* ``callbacks``: A :symbol:`mongoc_apm_callbacks_t`.
* ``cb``: A function to call with a :symbol:`mongoc_apm_server_opening_t` whenever the driver adds a :symbol:`mongoc_server_description_t` for a new server it was not monitoring before.

.. seealso::

  | :doc:`Introduction to Application Performance Monitoring <application-performance-monitoring>`

