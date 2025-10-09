:man_page: mongoc_server_api_destroy

mongoc_server_api_destroy()
===========================

Synopsis
--------

.. code-block:: c

  void
  mongoc_server_api_destroy (mongoc_server_api_t *api);

Free a :symbol:`mongoc_server_api_t`. Does nothing if ``api`` is NULL.
