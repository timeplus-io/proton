:man_page: mongoc_client_session_abort_transaction

mongoc_client_session_abort_transaction()
=========================================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_client_session_abort_transaction (mongoc_client_session_t *session,
                                           bson_error_t *error);


Abort a multi-document transaction.

Parameters
----------

* ``session``: A :symbol:`mongoc_client_session_t`.
* ``error``: An optional location for a :symbol:`bson_error_t <errors>` or ``NULL``.

Return
------

Returns true if the transaction was aborted. Returns ``false`` and sets ``error`` if there are invalid arguments, such as a session with no transaction in progress. Network or server errors are ignored.

.. only:: html

  .. include:: includes/seealso/session.txt
