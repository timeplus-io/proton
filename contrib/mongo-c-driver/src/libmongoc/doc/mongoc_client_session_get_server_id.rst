:man_page: mongoc_client_session_get_server_id

mongoc_client_session_get_server_id()
=====================================

Synopsis
--------

.. code-block:: c

  uint32_t
  mongoc_client_session_get_server_id (const mongoc_client_session_t *session);

Get the "server ID" of the ``mongos`` this :symbol:`mongoc_client_session_t` is pinned to.

Parameters
----------

* ``session``: A :symbol:`mongoc_client_session_t`.

Returns
-------

A server ID or ``0`` if this :symbol:`mongoc_client_session_t` is not pinned.

.. only:: html

  .. include:: includes/seealso/session.txt

