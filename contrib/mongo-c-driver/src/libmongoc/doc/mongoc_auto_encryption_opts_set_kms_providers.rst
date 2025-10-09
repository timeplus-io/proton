:man_page: mongoc_auto_encryption_opts_set_kms_providers

mongoc_auto_encryption_opts_set_kms_providers()
===============================================

Synopsis
--------

.. code-block:: c

   void
   mongoc_auto_encryption_opts_set_kms_providers (
      mongoc_auto_encryption_opts_t *opts, const bson_t *kms_providers);


Parameters
----------

* ``opts``: The :symbol:`mongoc_auto_encryption_opts_t`
* ``kms_providers``: A :symbol:`bson_t` containing configuration for an external Key Management Service (KMS).

``kms_providers`` is a BSON document containing configuration for each KMS provider.

KMS providers are specified as a string of the form ``<KMS provider type>`` or ``<KMS provider type>:<KMS provider name>``.
The supported KMS provider types are ``aws``, ``azure``, ``gcp``, ``local``, and ``kmip``. The optional name enables configuring multiple KMS providers with the same KMS provider type (e.g. ``aws:name1`` and ``aws:name2`` can refer to different AWS accounts).
At least one KMS provider must be specified.

The format for the KMS provider type ``aws`` is as follows:

.. code-block:: javascript

   aws: {
      accessKeyId: String,
      secretAccessKey: String
   }

The format for the KMS provider type ``local`` is as follows:

.. code-block:: javascript

   local: {
      key: <96 byte BSON binary of subtype 0> or String /* The master key used to encrypt/decrypt data keys. May be passed as a base64 encoded string. */
   }

The format for the KMS provider type ``azure`` is as follows:

.. code-block:: javascript

   azure: {
      tenantId: String,
      clientId: String,
      clientSecret: String,
      identityPlatformEndpoint: Optional<String> /* Defaults to login.microsoftonline.com */
   }

The format for the KMS provider type ``gcp`` is as follows:

.. code-block:: javascript

   gcp: {
      email: String,
      privateKey: byte[] or String, /* May be passed as a base64 encoded string. */
      endpoint: Optional<String> /* Defaults to oauth2.googleapis.com */
   }

The format for the KMS provider type ``kmip`` is as follows:

.. code-block:: javascript

   kmip: {
      endpoint: String
   }

KMS providers may include an optional name suffix separate with a colon. This enables configuring multiple KMS providers with the same KMS provider type. Example:

.. code-block:: javascript

   "aws:name1": {
      accessKeyId: String,
      secretAccessKey: String
   },
   "aws:name2": {
      accessKeyId: String,
      secretAccessKey: String
   }   

.. seealso::

  | :symbol:`mongoc_client_enable_auto_encryption()`

  | `In-Use Encryption <in-use-encryption_>`_

