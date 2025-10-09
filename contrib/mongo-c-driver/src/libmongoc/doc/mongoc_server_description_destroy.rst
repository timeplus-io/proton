:man_page: mongoc_server_description_destroy

mongoc_server_description_destroy()
===================================

Synopsis
--------

.. code-block:: c

  void
  mongoc_server_description_destroy (mongoc_server_description_t *description);

Parameters
----------

* ``description``: A :symbol:`mongoc_server_description_t`.

Description
-----------

Frees all resources associated with the server description. Does nothing if ``description`` is NULL.
