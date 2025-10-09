:man_page: mongoc_client_encryption_encrypt_range_opts_set_sparsity

mongoc_client_encryption_encrypt_range_opts_set_sparsity()
==========================================================

Synopsis
--------

.. code-block:: c

    void
    mongoc_client_encryption_encrypt_range_opts_set_sparsity (
         mongoc_client_encryption_encrypt_range_opts_t *range_opts, int64_t sparsity);

.. important:: The |qenc:range-is-experimental| |qenc:api-is-experimental|
.. versionadded:: 1.24.0

Sets sparsity for explicit encryption. Sparsity is required for explicit encryption of range indexes.
Only applies when the algorithm set by :symbol:`mongoc_client_encryption_encrypt_opts_set_algorithm()` is "RangePreview".
It is an error to set sparsity when algorithm is not "RangePreview".

Sparsity must match the value set in the encryptedFields of the destination collection.
It is an error to set a different value.

Parameters
----------

* ``range_opts``: A :symbol:`mongoc_client_encryption_encrypt_range_opts_t`
* ``sparsity``: A non-negative sparsity.

.. seealso::
    | :symbol:`mongoc_client_encryption_encrypt_range_opts_t`
