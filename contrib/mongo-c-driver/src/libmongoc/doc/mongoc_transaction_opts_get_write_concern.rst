:man_page: mongoc_transaction_opts_get_write_concern

mongoc_transaction_opts_get_write_concern()
===========================================

Synopsis
--------

.. code-block:: c

  const mongoc_write_concern_t *
  mongoc_transaction_opts_get_write_concern (const mongoc_transaction_opt_t *opts);

Return the transaction options' :symbol:`mongoc_write_concern_t`. See :symbol:`mongoc_transaction_opts_set_write_concern()`.

Parameters
----------

* ``opts``: A :symbol:`mongoc_transaction_opt_t`.

Returns
-------

A  :symbol:`mongoc_write_concern_t` that is valid only for the lifetime of ``opts``.

.. only:: html

  .. include:: includes/seealso/session.txt
