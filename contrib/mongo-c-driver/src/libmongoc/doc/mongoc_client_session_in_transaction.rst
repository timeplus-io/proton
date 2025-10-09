:man_page: mongoc_client_session_in_transaction

mongoc_client_session_in_transaction()
======================================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_client_session_in_transaction (const mongoc_client_session_t *session);

Check whether a multi-document transaction is in progress for this session.

Parameters
----------

* ``session``: A :symbol:`mongoc_client_session_t`.

Return
------

Returns true if a transaction was started and has not been committed or aborted, otherwise ``false``.
