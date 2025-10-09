:man_page: mongoc_session_opts_get_transaction_opts

mongoc_session_opts_get_transaction_opts()
==========================================

Synopsis
--------

.. code-block:: c

  mongoc_transaction_opt_t *
  mongoc_session_opts_get_transaction_opts (
     const mongoc_client_session_t *session) BSON_GNUC_WARN_UNUSED_RESULT;

The options for the current transaction started with this session.

If this ``session`` is not in a transaction, then the returned value is ``NULL``. See :symbol:`mongoc_client_session_in_transaction()`. 

Parameters
----------

* ``session``: A :symbol:`mongoc_client_session_t`.

Returns
-------

If the session is in a transaction, a new :symbol:`mongoc_transaction_opt_t` that must be freed with :symbol:`mongoc_transaction_opts_destroy()`. Otherwise, ``NULL``.

.. only:: html

  .. include:: includes/seealso/session.txt

