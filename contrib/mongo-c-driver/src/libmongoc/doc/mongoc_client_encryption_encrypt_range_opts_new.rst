:man_page: mongoc_client_encryption_encrypt_range_opts_new

mongoc_client_encryption_encrypt_range_opts_new()
=================================================

Synopsis
--------

.. code-block:: c

  mongoc_client_encryption_encrypt_range_opts_t *
  mongoc_client_encryption_encrypt_range_opts_new (void);

.. important:: The |qenc:range-is-experimental| |qenc:api-is-experimental|
.. versionadded:: 1.24.0

Returns
-------

A new :symbol:`mongoc_client_encryption_encrypt_range_opts_t` that must be freed with :symbol:`mongoc_client_encryption_encrypt_range_opts_destroy()`.


.. seealso::
    | :symbol:`mongoc_client_encryption_encrypt_range_opts_t`
    | :symbol:`mongoc_client_encryption_encrypt_range_opts_destroy`
