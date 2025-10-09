:man_page: mongoc_client_session_get_lsid

mongoc_client_session_get_lsid()
================================

Synopsis
--------

.. code-block:: c

  const bson_t *
  mongoc_client_session_get_lsid (mongoc_client_session_t *session);

Get the server-side "logical session ID" associated with this :symbol:`mongoc_client_session_t` as a BSON document.

Parameters
----------

* ``session``: A :symbol:`mongoc_client_session_t`.

Returns
-------

A :symbol:`bson:bson_t` that is valid only for the lifetime of ``session``.

.. only:: html

  .. include:: includes/seealso/session.txt
