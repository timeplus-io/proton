:man_page: mongoc_transaction_opts_destroy

mongoc_transaction_opts_destroy()
=================================

Synopsis
--------

.. code-block:: c

  void
  mongoc_transaction_opts_destroy (mongoc_transaction_opt_t *opts);

Free a :symbol:`mongoc_transaction_opt_t`. Does nothing if ``opts`` is NULL.

Parameters
----------

* ``opts``: A :symbol:`mongoc_transaction_opt_t`.

.. only:: html

  .. include:: includes/seealso/session.txt
