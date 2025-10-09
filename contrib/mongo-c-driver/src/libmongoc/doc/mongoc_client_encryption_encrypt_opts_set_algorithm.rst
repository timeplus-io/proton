:man_page: mongoc_client_encryption_encrypt_opts_set_algorithm

mongoc_client_encryption_encrypt_opts_set_algorithm()
=====================================================

Synopsis
--------

.. code-block:: c

   void
   mongoc_client_encryption_encrypt_opts_set_algorithm (
      mongoc_client_encryption_encrypt_opts_t *opts, const char *algorithm);

   #define MONGOC_AEAD_AES_256_CBC_HMAC_SHA_512_RANDOM "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
   #define MONGOC_AEAD_AES_256_CBC_HMAC_SHA_512_DETERMINISTIC "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
   // (Experimental: See below)
   #define MONGOC_ENCRYPT_ALGORITHM_INDEXED "Indexed"
   // (Experimental: See below)
   #define MONGOC_ENCRYPT_ALGORITHM_UNINDEXED "Unindexed"
   // (Experimental: See below)
   #define MONGOC_ENCRYPT_ALGORITHM_RANGEPREVIEW "RangePreview"

Identifies the algorithm to use for encryption. Valid values of ``algorithm`` are:

``"AEAD_AES_256_CBC_HMAC_SHA_512-Random"``

   for randomized encryption. Specific to the `Client-Side Field Level Encryption <client-side-field-level-encryption_>`_ feature.

``"AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"``

   for deterministic (queryable) encryption. Specific to the `Client-Side Field Level Encryption <client-side-field-level-encryption_>`_ feature.

``"Indexed"``

   for indexed encryption. Specific to the `Queryable Encryption <queryable-encryption_>`_ feature.

``"Unindexed"``

   for unindexed encryption. Specific to the `Queryable Encryption <queryable-encryption_>`_ feature.

``"RangePreview"``

   for range encryption. Specific to the `Queryable Encryption <queryable-encryption_>`_ feature.
   
   .. note:: The |qenc:range-is-experimental| |qenc:opt-is-experimental|

Parameters
----------

* ``opts``: A :symbol:`mongoc_client_encryption_encrypt_opts_t`
* ``algorithm``: A ``char *`` identifying the algorithm.
