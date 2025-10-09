:man_page: mongoc_client_session_destroy

mongoc_client_session_destroy()
===============================

Synopsis
--------

.. code-block:: c

  void
  mongoc_client_session_destroy (mongoc_client_session_t *session);

End a session, returning its session id to the pool, and free all client resources associated with the session. If a multi-document transaction is in progress, abort it. Does nothing if ``session`` is NULL.

See the example code for :symbol:`mongoc_client_session_t`.

Parameters
----------

* ``session``: A :symbol:`mongoc_client_session_t`.

.. only:: html

  .. include:: includes/seealso/session.txt
