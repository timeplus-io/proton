:man_page: mongoc_client_encryption_rewrap_many_datakey_result_destroy

mongoc_client_encryption_rewrap_many_datakey_result_destroy()
=============================================================

Synopsis
--------

.. code-block:: c

   void
   mongoc_client_encryption_rewrap_many_datakey_result_destroy (
      mongoc_client_encryption_rewrap_many_datakey_result_t *result);

Frees resources of a :symbol:`mongoc_client_encryption_rewrap_many_datakey_result_t` created with :symbol:`mongoc_client_encryption_rewrap_many_datakey_result_new()`. Does nothing if ``NULL`` is passed.

Parameters
----------

* ``result``: A :symbol:`mongoc_client_encryption_rewrap_many_datakey_result_t`.
