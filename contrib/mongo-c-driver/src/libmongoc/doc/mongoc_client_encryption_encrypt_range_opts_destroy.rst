:man_page: mongoc_client_encryption_encrypt_range_opts_destroy

mongoc_client_encryption_encrypt_range_opts_destroy()
=====================================================

Synopsis
--------

.. code-block:: c

  void
  mongoc_client_encryption_encrypt_range_opts_destroy (mongoc_client_encryption_encrypt_range_opts_t *range_opts);

.. important:: The |qenc:range-is-experimental| |qenc:api-is-experimental|
.. versionadded:: 1.24.0
    
Frees resources of a :symbol:`mongoc_client_encryption_encrypt_range_opts_t` created with :symbol:`mongoc_client_encryption_encrypt_range_opts_new()`. Does nothing if ``NULL`` is passed.

Parameters
----------

* ``range_opts``: A :symbol:`mongoc_client_encryption_encrypt_range_opts_t`.

.. seealso::
    | :symbol:`mongoc_client_encryption_encrypt_range_opts_t`