:man_page: mongoc_transaction_opts_set_max_commit_time_ms

mongoc_transaction_opts_set_max_commit_time_ms()
================================================

Synopsis
--------

.. code-block:: c

  void
  mongoc_transaction_opts_set_max_commit_time_ms (mongoc_transaction_opt_t *opts,
                                                  int64_t max_commit_time_ms);

Configure the transaction's max commit time, in milliseconds.

Parameters
----------

* ``opts``: A :symbol:`mongoc_transaction_opt_t`.
* ``int64_t``: Timeout for commitTransaction, in milliseconds.

.. only:: html

  .. include:: includes/seealso/session.txt
