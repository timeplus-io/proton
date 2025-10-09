:man_page: mongoc_client_encryption_create_encrypted_collection

mongoc_client_encryption_create_encrypted_collection()
======================================================

Synopsis
--------

.. code-block:: c

  mongoc_collection_t*
  mongoc_client_encryption_create_encrypted_collection (
        mongoc_client_encryption_t *enc,
        mongoc_database_t *database,
        const char *name,
        const bson_t *in_options,
        bson_t *out_options,
        const char *kms_provider,
        const bson_t *opt_masterKey,
        bson_error_t *error)
    BSON_GNUC_WARN_UNUSED_RESULT;

Create a new collection with `Queryable Encryption <queryable-encryption_>`_
enabled. Requires a valid :symbol:`mongoc_client_encryption_t` object to
operate.

.. versionadded:: 1.24.0

.. seealso::

    This function is a convenience API wrapping
    :symbol:`mongoc_database_create_collection`.


Parameters
----------

* ``enc``: The :symbol:`mongoc_client_encryption_t` to be used to configure
  encryption for the new collection.
* ``database``: The :symbol:`mongoc_database_t` in which the new collection will
  be created.
* ``name``: The name of the new collection.
* ``in_options``: The options for the new collection. (See below).
* ``out_options``: An optional output option for the final create-collection
  options. Should point to storage for a :symbol:`bson_t`. The pointed-to object
  must be destroyed by the caller. If ``NULL``, has no effect.
* ``kms_provider``: The name of the KMS provider to use for generating new data
  encryption keys for encrypted fields within the collection.
* ``opt_masterKey``: If provided, used as the masterkey option when data
  encryption keys need to be created. (See:
  :doc:`mongoc_client_encryption_datakey_opts_set_masterkey`)
* ``error``: Optional output parameter pointing to storage for a
  :symbol:`bson_error_t`. If an error occurs, will be initialized with error
  information.


Returns
-------

If successful, this function returns a new :symbol:`mongoc_collection_t` object.
Upon failure, returns ``NULL`` and initializes ``*error`` with an error
indicating the reason for failure. The returned collection object must be freed
by the caller.


Creation Options
----------------

The ``in_options`` parameter behaves similarly to the ``opts`` parameter for
:symbol:`mongoc_database_create_collection`, which accepts the options for the
``create`` MongoDB command
(`Documented here <https://www.mongodb.com/docs/manual/reference/command/create>`_).
The ``in_options`` document accepted here is different in one important way:

The ``$.encryptedFields.fields`` array is *required* by this function, and,
unlike the schema documented for the ``create`` command, accepts a value of
``null`` for the ``keyId`` parameter on each array element.

This function has the following as-if effect:

.. default-role:: math

1. A new set of options `O` will be created based on ``in_options``.
2. For each element `F` in the ``$.encryptedFields.fields`` array of `O`:

   1. If `F` contains a ``"keyId": null`` element, a new data encryption key
      `K_f` will be created as-if by calling the
      :symbol:`mongoc_client_encryption_create_datakey`, using the relevant
      arguments that were given to
      ``mongoc_client_encryption_create_encrypted_collection``.
   2. The ID of `K_f` will be used to replace the ``"keyId": null`` element
      within `F`.

3. A collection will be created using the options `O`.
4. If ``out_options`` is not ``NULL``, `O` will be written to
   ``out_options``.
