:man_page: mongoc_client_encryption_destroy

mongoc_client_encryption_destroy()
==================================

Synopsis
--------

.. code-block:: c

   void
   mongoc_client_encryption_destroy (
      mongoc_client_encryption_t *client_encryption);

Release all resources associated with ``client_encryption`` and free the structure. Does nothing if ``client_encryption`` is NULL.

Parameters
----------

* ``client_encryption``: A :symbol:`mongoc_client_encryption_t`.
