:man_page: mongoc_bulk_operation_set_client_session

mongoc_bulk_operation_set_client_session()
==========================================

Synopsis
--------

.. code-block:: c

   void
   mongoc_bulk_operation_set_client_session (
     mongoc_bulk_operation_t *bulk,
     mongoc_client_session_t *client_session);

Sets an explicit client session to use for the bulk operation.

It is an error to use a session for unacknowledged writes.

Parameters
----------

* ``bulk``: A :symbol:`mongoc_bulk_operation_t`.
* ``client_session``: A :symbol:`mongoc_client_session_t`. Must be derived from the same :symbol:`mongoc_client_t` as ``bulk``.

.. seealso::

  | :symbol:`mongoc_client_start_session()`

