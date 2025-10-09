:man_page: mongoc_client_session_with_transaction

mongoc_client_session_with_transaction()
========================================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_client_session_with_transaction (mongoc_client_session_t *session,
		                          mongoc_client_session_with_transaction_cb_t cb,
					  const mongoc_transaction_opt_t *opts,
					  void *ctx,
					  bson_t *reply,
					  bson_error_t *error);

This method will start a new transaction on ``session``, run ``cb``, and then commit the transaction. If it cannot commit the transaction, the entire sequence may be retried, and ``cb`` may be run multiple times. ``ctx`` will be passed to ``cb`` each time it is called.

This method has an internal time limit of 120 seconds, and will retry until that time limit is reached. This timeout is not configurable.

``cb`` should not attempt to start new transactions, but should simply run operations meant to be contained within a transaction. The ``cb`` does not need to commit transactions; this is handled by the :symbol:`mongoc_client_session_with_transaction`. If ``cb`` does commit or abort a transaction, however, this method will return without taking further action.

The parameter ``reply`` is initialized even upon failure to simplify memory management.

Parameters
----------

* ``session``: A :symbol:`mongoc_client_session_t`.
* ``cb``: A :symbol:`mongoc_client_session_with_transaction_cb_t` callback, which will run inside of a new transaction on the session. See example below.
* ``opts``: An optional :symbol:`mongoc_transaction_opt_t`.
* ``ctx``: A ``void*``. This user-provided data will be passed to ``cb``.
* ``reply``: A |bson_t-opt-storage-ptr| to contain the results.
* ``error``: An optional location for a :symbol:`bson_error_t` or ``NULL``.

Return
------

Returns ``true`` if the transaction was completed successfully.  Otherwise, returns ``false`` in case of failure.  In cases of failure ``error`` will also be set, except if the passed-in ``cb`` fails without setting ``error``.  If a non-NULL ``reply`` is passed in, ``reply`` will be set to the value of the last server response, except if the passed-in ``cb`` fails without setting a ``reply``.

Example
-------

.. literalinclude:: ../examples/example-with-transaction-cb.c
   :language: c
   :caption: Use with_transaction() to run a callback within a transaction
