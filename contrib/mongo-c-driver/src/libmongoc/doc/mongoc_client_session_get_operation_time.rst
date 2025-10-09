:man_page: mongoc_client_session_get_operation_time

mongoc_client_session_get_operation_time()
==========================================

Synopsis
--------

.. code-block:: c

  void
  mongoc_client_session_get_operation_time (const mongoc_client_session_t *session,
                                            uint32_t *timestamp,
                                            uint32_t *increment);

Get the session's operationTime, expressed as a BSON Timestamp with timestamp and increment components. If the session has not been used for any operations, the timestamp and increment are 0.

Parameters
----------

* ``session``: A :symbol:`mongoc_client_session_t`.
* ``timestamp``: A pointer to a ``uint32_t`` to receive the timestamp component.
* ``increment``: A pointer to a ``uint32_t`` to receive the increment component.

.. only:: html

  .. include:: includes/seealso/session.txt
