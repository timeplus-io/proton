:man_page: mongoc_client_encryption_datakey_opts_destroy

mongoc_client_encryption_datakey_opts_destroy()
===============================================

Synopsis
--------

.. code-block:: c

  void
  mongoc_client_encryption_datakey_opts_destroy (mongoc_client_encryption_datakey_opts_t *opts);

Frees resources of a :symbol:`mongoc_client_encryption_datakey_opts_t` created with :symbol:`mongoc_client_encryption_datakey_opts_new()`. Does nothing if ``NULL`` is passed.

Parameters
----------

* ``opts``: A :symbol:`mongoc_client_encryption_datakey_opts_t`.