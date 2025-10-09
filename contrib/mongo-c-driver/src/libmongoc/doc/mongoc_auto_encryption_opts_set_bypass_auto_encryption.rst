:man_page: mongoc_auto_encryption_opts_set_bypass_auto_encryption

mongoc_auto_encryption_opts_set_bypass_auto_encryption()
========================================================

Synopsis
--------

.. code-block:: c

   void
   mongoc_auto_encryption_opts_set_bypass_auto_encryption (
      mongoc_auto_encryption_opts_t *opts, bool bypass_auto_encryption);


Parameters
----------

* ``opts``: The :symbol:`mongoc_auto_encryption_opts_t`
* ``bypass_auto_encryption``: A boolean. If true, a :symbol:`mongoc_client_t` configured with :symbol:`mongoc_client_enable_auto_encryption()` will only perform automatic decryption (not encryption).

.. seealso::

  | :symbol:`mongoc_client_enable_auto_encryption()`

  | `In-Use Encryption <in-use-encryption_>`_

