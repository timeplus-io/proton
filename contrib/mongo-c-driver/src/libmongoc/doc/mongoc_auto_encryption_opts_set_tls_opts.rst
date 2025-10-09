:man_page: mongoc_auto_encryption_opts_set_tls_opts

mongoc_auto_encryption_opts_set_tls_opts()
==========================================

Synopsis
--------

.. code-block:: c

   void
   mongoc_auto_encryption_opts_set_tls_opts (
      mongoc_auto_encryption_opts_t *opts, const bson_t *tls_opts);


Parameters
----------

* ``opts``: The :symbol:`mongoc_auto_encryption_opts_t`
* ``tls_opts``: A :symbol:`bson_t` mapping a Key Management Service (KMS) provider to a BSON document with TLS options.

``tls_opts`` is a BSON document of the following form:

.. code-block:: javascript

   <KMS provider>: {
      tlsCaFile: Optional<String>
      tlsCertificateKeyFile: Optional<String>
      tlsCertificateKeyFilePassword: Optional<String>
   }

The KMS providers ``aws``, ``azure``, ``gcp``, and ``kmip`` are supported as keys in the ``tls_opts`` document. They may include an optional name suffix separated with a colon. Example: ``aws:name2``.

``tls_opts`` maps the KMS provider name to a BSON document for TLS options.

The BSON document for TLS options may contain the following keys:

- ``MONGOC_URI_TLSCERTIFICATEKEYFILE``
- ``MONGOC_URI_TLSCERTIFICATEKEYFILEPASSWORD``
- ``MONGOC_URI_TLSCAFILE``

.. literalinclude:: ../examples/client-side-encryption-doc-snippets.c
   :caption: Example use
   :start-after: BEGIN:mongoc_auto_encryption_opts_set_tls_opts
   :end-before: END:mongoc_auto_encryption_opts_set_tls_opts
   :dedent: 6

See `Configuring TLS <configuring_tls_>`_ for a description of the behavior of these options.

.. seealso::

  | :symbol:`mongoc_client_enable_auto_encryption()`

  | `In-Use Encryption <in-use-encryption_>`_

