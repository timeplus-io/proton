:man_page: mongoc_client_encryption_encrypt_opts_set_contention_factor

mongoc_client_encryption_encrypt_opts_set_contention_factor()
=============================================================

Synopsis
--------

.. code-block:: c

    void
    mongoc_client_encryption_encrypt_opts_set_contention_factor (
        mongoc_client_encryption_encrypt_opts_t *opts, int64_t contention_factor);

.. versionadded:: 1.22.0

Sets a contention factor for explicit encryption.
Only applies when the algorithm set by :symbol:`mongoc_client_encryption_encrypt_opts_set_algorithm()` is "Indexed" or "RangePreview".
It is an error to set the contention factor when algorithm is not "Indexed" or "RangePreview". **Note** that the |qenc:range-is-experimental|
If contention factor is not supplied, it defaults to a value of 0.

Parameters
----------

* ``opts``: A :symbol:`mongoc_client_encryption_encrypt_opts_t`
* ``contention_factor``: A non-negative contention factor.
