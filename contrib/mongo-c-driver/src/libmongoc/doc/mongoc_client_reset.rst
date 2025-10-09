:man_page: mongoc_client_reset

mongoc_client_reset()
=====================

Synopsis
--------

.. code-block:: c

  void
  mongoc_client_reset (mongoc_client_t *client);

Call this method in the child after forking to invalidate the :symbol:`mongoc_client_t`.

Description
-----------

Calling :symbol:`mongoc_client_reset()` prevents resource cleanup in the child process from interfering with the parent process.

This method causes the client to clear its session pool without sending endSessions. It also increments an internal generation counter on the given client. After this method is called, cursors from previous generations will not issue a killCursors command when they are destroyed. Client sessions from previous generations cannot be used and should be destroyed.

Parameters
----------

* ``client``: A :symbol:`mongoc_client_t`.

