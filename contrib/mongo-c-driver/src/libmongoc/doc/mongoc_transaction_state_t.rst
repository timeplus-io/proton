:man_page: mongoc_transaction_state_t

mongoc_transaction_state_t
==========================

Constants for transaction states

Synopsis
--------

.. code-block:: c

  typedef enum {
    MONGOC_TRANSACTION_NONE = 0,
    MONGOC_TRANSACTION_STARTING = 1,
    MONGOC_TRANSACTION_IN_PROGRESS = 2,
    MONGOC_TRANSACTION_COMMITTED = 3,
    MONGOC_TRANSACTION_ABORTED = 4,
  } mongoc_transaction_state_t;

Description
-----------

These constants describe the current transaction state of a session.

Flag Values
-----------

==================================  =============================================================================
MONGOC_TRANSACTION_NONE             There is no transaction in progress.
MONGOC_TRANSACTION_STARTING         A transaction has been started, but no operation has been sent to the server.
MONGOC_TRANSACTION_IN_PROGRESS      A transaction is in progress.
MONGOC_TRANSACTION_COMMITTED        The transaction was committed.
MONGOC_TRANSACTION_ABORTED          The transaction was aborted.
==================================  =============================================================================
