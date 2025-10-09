:man_page: mongoc_client_encryption_opts_set_tls_opts

mongoc_client_encryption_opts_set_tls_opts()
============================================

Synopsis
--------

.. code-block:: c

   void
   mongoc_client_encryption_opts_set_tls_opts (
      mongoc_client_encryption_opts_t *opts, const bson_t *tls_opts);


Parameters
----------

* ``opts``: The :symbol:`mongoc_client_encryption_opts_t`
* ``tls_opts``: A :symbol:`bson_t` mapping a Key Management Service (KMS) provider to a BSON document with TLS options.

``tls_opts`` is a BSON document of the following form:

.. code-block:: javascript

   <KMS provider>: {
      tlsCaFile: Optional<String>
      tlsCertificateKeyFile: Optional<String>
      tlsCertificateKeyFilePassword: Optional<String>
   }

KMS providers are specified as a string of the form ``<KMS provider type>`` or ``<KMS provider type>:<KMS provider name>``.
The supported KMS provider types are ``aws``, ``azure``, ``gcp``, ``local``, and ``kmip``. The optional name enables configuring multiple KMS providers with the same KMS provider type (e.g. ``aws:name1`` and ``aws:name2`` can refer to different AWS accounts).

``tls_opts`` maps the KMS provider to a BSON document for TLS options.

The BSON document for TLS options may contain the following keys:

- ``MONGOC_URI_TLSCERTIFICATEKEYFILE``
- ``MONGOC_URI_TLSCERTIFICATEKEYFILEPASSWORD``
- ``MONGOC_URI_TLSCAFILE``

.. literalinclude:: ../examples/client-side-encryption-doc-snippets.c
   :caption: Example use
   :start-after: BEGIN:mongoc_client_encryption_opts_set_tls_opts
   :end-before: END:mongoc_client_encryption_opts_set_tls_opts
   :dedent: 6

See `Configuring TLS <configuring_tls_>`_ for a description of the behavior of these options.

.. seealso::

  | `In-Use Encryption <in-use-encryption_>`_

