:man_page: mongoc_auto_encryption_opts_set_bypass_query_analysis

mongoc_auto_encryption_opts_set_bypass_query_analysis()
=======================================================

Synopsis
--------

.. code-block:: c

   void
   mongoc_auto_encryption_opts_set_bypass_query_analysis (
      mongoc_auto_encryption_opts_t *opts, bool bypass_query_analysis);

.. versionadded:: 1.22.0

Parameters
----------

* ``opts``: The :symbol:`mongoc_auto_encryption_opts_t`
* ``bypass_query_analysis``: A boolean.


``bypass_query_analysis`` disables automatic analysis of outgoing commands.
``bypass_query_analysis`` is useful for encrypting indexed fields without the
``crypt_shared`` library or ``mongocryptd`` process. Set
``bypass_query_analysis`` to true to use explicit encryption on indexed fields.

.. seealso::

  | :symbol:`mongoc_client_enable_auto_encryption()`

  | `In-Use Encryption <in-use-encryption_>`_

