:man_page: mongoc_transaction_opts_set_write_concern

mongoc_transaction_opts_set_write_concern()
===========================================

Synopsis
--------

.. code-block:: c

  void
  mongoc_transaction_opts_set_write_concern (mongoc_transaction_opt_t *opts,
                                             const mongoc_write_concern_t *write_concern);

Configure the transaction's write concern. The argument is copied into the struct and can be freed after calling this function.

Parameters
----------

* ``opts``: A :symbol:`mongoc_transaction_opt_t`.
* ``write_concern``: A :symbol:`mongoc_write_concern_t`.

.. only:: html

  .. include:: includes/seealso/session.txt
