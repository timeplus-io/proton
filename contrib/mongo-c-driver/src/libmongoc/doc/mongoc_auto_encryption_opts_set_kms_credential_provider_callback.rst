:man_page: mongoc_auto_encryption_opts_set_kms_credential_provider_callback

mongoc_auto_encryption_opts_set_kms_credential_provider_callback()
==================================================================

.. versionadded:: 1.23.0

Synopsis
--------

.. code-block:: c

  void
  mongoc_auto_encryption_opts_set_kms_credential_provider_callback(
    mongoc_auto_encryption_opts_t *opts,
    mongoc_kms_credentials_provider_callback_fn fn,
    void *userdata);

Set the user-provided callback to provide KMS credentials on-demand when they
are needed.

Parameters
----------

- ``opts`` - The options object to update.
- ``fn`` - The provider callback to set on the options object. May be ``NULL``
  to clear the callback. Refer to:
  :c:type:`mongoc_kms_credentials_provider_callback_fn`
- ``userdata`` - An arbitrary pointer that will be passed along to the
  callback function when it is called by libmongoc.

.. seealso:: :doc:`mongoc_client_encryption_opts_set_kms_credential_provider_callback`
