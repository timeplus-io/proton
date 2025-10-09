:man_page: mongoc_client_session_with_transaction_cb_t

mongoc_client_session_with_transaction_cb_t
===========================================

Synopsis
--------

.. code-block:: c

  typedef bool (*mongoc_client_session_with_transaction_cb_t) (
     mongoc_client_session_t *session,
     void *ctx,
     bson_t **reply,
     bson_error_t *error);

Provide this callback to :symbol:`mongoc_client_session_with_transaction`. The callback should run a sequence of operations meant to be contained within a transaction.  The callback should not attempt to start or commit transactions.

Parameters
----------

* ``session``: A :symbol:`mongoc_client_session_t`.
* ``ctx``: A ``void*`` set to the the user-provided ``ctx`` passed to :symbol:`mongoc_client_session_with_transaction`.
* ``reply``: An optional location for a :symbol:`bson_t` or ``NULL``. The callback should set this if it runs any operations against the server and receives replies.
* ``error``: A :symbol:`bson_error_t`. The callback should set this if it receives any errors while running operations against the server.

Return
------

Returns ``true`` for success and ``false`` on failure. If ``cb`` returns ``false`` then it should also set ``error``.

.. seealso::

  | :symbol:`mongoc_client_session_with_transaction`

