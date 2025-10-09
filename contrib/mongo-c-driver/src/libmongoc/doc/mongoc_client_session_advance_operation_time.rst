:man_page: mongoc_client_session_advance_operation_time

mongoc_client_session_advance_operation_time()
==============================================

Synopsis
--------

.. code-block:: c

  void
  mongoc_client_session_advance_operation_time (mongoc_client_session_t *session,
                                                uint32_t timestamp,
                                                uint32_t increment);

Advance the session's operation time, expressed as a BSON Timestamp with timestamp and increment components. Has an effect only if the new operation time is greater than the session's current operation time.

Use :symbol:`mongoc_client_session_advance_operation_time` and :symbol:`mongoc_client_session_advance_cluster_time` to copy the operationTime and clusterTime from another session, ensuring subsequent operations in this session are causally consistent with the last operation in the other session

Parameters
----------

* ``session``: A :symbol:`mongoc_client_session_t`.
* ``timestamp``: The new operationTime's timestamp component.
* ``increment``: The new operationTime's increment component.

.. only:: html

  .. include:: includes/seealso/session.txt
