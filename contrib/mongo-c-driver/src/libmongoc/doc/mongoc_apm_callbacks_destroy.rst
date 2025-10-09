:man_page: mongoc_apm_callbacks_destroy

mongoc_apm_callbacks_destroy()
==============================

Synopsis
--------

.. code-block:: c

  void
  mongoc_apm_callbacks_destroy (mongoc_apm_callbacks_t *callbacks);

Free a :symbol:`mongoc_apm_callbacks_t`. Does nothing if ``callbacks`` is NULL.

.. seealso::

  | :doc:`Introduction to Application Performance Monitoring <application-performance-monitoring>`

