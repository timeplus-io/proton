:man_page: mongoc_error_has_label

mongoc_error_has_label()
========================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_error_has_label (const bson_t *reply, const char *label);

Test whether a reply from a failed operation includes a specific error label. See :ref:`Error Labels <error_labels>` for details, and see :symbol:`mongoc_client_session_start_transaction` for example code that demonstrates their use.

Parameters
----------

* ``reply``: A |bson_t-storage-ptr| to contain the results.
* ``label``: The label to test for, such as "TransientTransactionError" or "UnknownTransactionCommitResult".

Returns
-------

Returns true if ``reply`` contains the error label.
