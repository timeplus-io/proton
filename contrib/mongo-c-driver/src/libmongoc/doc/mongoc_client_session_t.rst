:man_page: mongoc_client_session_t
:tags: session

mongoc_client_session_t
=======================

Use a session for a sequence of operations, optionally with causal consistency. See `the MongoDB Manual Entry for Causal Consistency <https://www.mongodb.com/docs/manual/core/read-isolation-consistency-recency/#causal-consistency>`_.

Synopsis
--------

.. include:: includes/session-lifecycle.txt

Fork Safety
-----------
A :symbol:`mongoc_client_session_t` is only usable in the parent process after a fork. The child process must call :symbol:`mongoc_client_reset` on the ``client`` field.

Example
-------

.. literalinclude:: ../examples/example-session.c
   :language: c
   :caption: example-session.c

.. only:: html

  Functions
  ---------

  .. toctree::
    :titlesonly:
    :maxdepth: 1

    mongoc_client_session_start_transaction
    mongoc_client_session_in_transaction
    mongoc_client_session_get_transaction_state
    mongoc_client_session_commit_transaction
    mongoc_client_session_abort_transaction
    mongoc_client_session_advance_cluster_time
    mongoc_client_session_advance_operation_time
    mongoc_client_session_with_transaction
    mongoc_client_session_append
    mongoc_client_session_get_client
    mongoc_client_session_get_cluster_time
    mongoc_client_session_get_dirty
    mongoc_client_session_get_operation_time
    mongoc_client_session_get_opts
    mongoc_client_session_get_lsid
    mongoc_client_session_get_server_id
    mongoc_client_session_destroy
