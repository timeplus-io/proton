:man_page: mongoc_client_encryption_encrypt_range_opts_set_precision

mongoc_client_encryption_encrypt_range_opts_set_precision()
===========================================================

Synopsis
--------

.. code-block:: c

    void
    mongoc_client_encryption_encrypt_range_opts_set_precision (
         mongoc_client_encryption_encrypt_range_opts_t *range_opts, int32_t precision);

.. important:: The |qenc:range-is-experimental| |qenc:api-is-experimental|
.. versionadded:: 1.24.0

Sets precision for explicit encryption.
Only applies when the algorithm set by :symbol:`mongoc_client_encryption_encrypt_opts_set_algorithm()` is "RangePreview".
It is an error to set precision when algorithm is not "RangePreview".

Precision can only be set with double or decimal128 fields. 
It is an error to set precision if the type of the encryptedFields in the destination collection is not double or decimal128. 

For double and decimal128 fields, min/max/precision must all be set, or all be unset.

Precision must match the value set in the encryptedFields of the destination collection.
It is an error to set a different value.

Parameters
----------

* ``range_opts``: A :symbol:`mongoc_client_encryption_encrypt_range_opts_t`
* ``precision``: A non-negative precision. 

.. seealso::

  | :symbol:`mongoc_client_encryption_encrypt_range_opts_set_min`
  | :symbol:`mongoc_client_encryption_encrypt_range_opts_set_max`
  | :symbol:`mongoc_client_encryption_encrypt_range_opts_t`
