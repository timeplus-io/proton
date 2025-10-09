:man_page: mongoc_auto_encryption_opts_t

mongoc_auto_encryption_opts_t
=============================

Options for enabling automatic encryption and decryption for `In-Use Encryption <in-use-encryption_>`_.

Synopsis
--------

.. code-block:: c

  typedef struct _mongoc_auto_encryption_opts_t mongoc_auto_encryption_opts_t;

.. seealso::

  | `In-Use Encryption <in-use-encryption_>`_

.. only:: html

  Functions
  ---------

  .. toctree::
    :titlesonly:
    :maxdepth: 1

    mongoc_auto_encryption_opts_new
    mongoc_auto_encryption_opts_destroy
    mongoc_auto_encryption_opts_set_keyvault_client
    mongoc_auto_encryption_opts_set_keyvault_client_pool
    mongoc_auto_encryption_opts_set_keyvault_namespace
    mongoc_auto_encryption_opts_set_kms_providers
    mongoc_auto_encryption_opts_set_kms_credential_provider_callback
    mongoc_auto_encryption_opts_set_schema_map
    mongoc_auto_encryption_opts_set_bypass_auto_encryption
    mongoc_auto_encryption_opts_set_extra
    mongoc_auto_encryption_opts_set_tls_opts
    mongoc_auto_encryption_opts_set_encrypted_fields_map
    mongoc_auto_encryption_opts_set_bypass_query_analysis

