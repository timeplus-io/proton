:man_page: mongoc_client_encryption_opts_set_kms_credential_provider_callback

mongoc_client_encryption_opts_set_kms_credential_provider_callback ()
=====================================================================

.. versionadded:: 1.23.0

Synopsis
--------

.. code-block:: c

  void
  mongoc_client_encryption_opts_set_kms_credential_provider_callback (
    mongoc_client_encryption_opts_t *opts,
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

.. seealso:: :doc:`mongoc_auto_encryption_opts_set_kms_credential_provider_callback`

.. rubric:: Related:

.. c:type:: mongoc_kms_credentials_provider_callback_fn

  .. -
    The :noindexentry: prevents a one-off index entry for this item.
    Most entities are not documented as Sphinx objects, and thus do not generate
    index entries. Future changes may flip the script.

  .. code-block:: c

    typedef
    bool (*mongoc_kms_credentials_provider_callback_fn) (void *userdata,
                                                         const bson_t *params,
                                                         bson_t *out,
                                                         bson_error_t *error);

  The type of a callback function for providing KMS providers data on-demand.

  :parameters:

    - ``userdata`` - The same userdata pointer provided to the ``userdata``
      parameter when the callback was set.
    - ``params`` - Parameters for the requested KMS credentials. Currently
      empty.
    - ``out`` - The output :symbol:`bson:bson_t` in which to write the new
      KMS providers. When passed to the callback, this already points to an
      empty BSON document which must be populated.
    - ``error`` - An output parameter for indicating any errors that might
      occur while generating the KMS credentials.

  :return value: Must return ``true`` on success, ``false`` on failure.
