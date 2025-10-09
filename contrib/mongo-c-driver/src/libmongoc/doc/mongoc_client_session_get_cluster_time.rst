:man_page: mongoc_client_session_get_cluster_time

mongoc_client_session_get_cluster_time()
========================================

Synopsis
--------

.. code-block:: c

  const bson_t *
  mongoc_client_session_get_cluster_time (const mongoc_client_session_t *session);

Get the session's clusterTime as a BSON document.

Parameters
----------

* ``session``: A :symbol:`mongoc_client_session_t`.

Returns
-------

If the session has not been used for any operation and :symbol:`mongoc_client_session_advance_cluster_time()` has not been called, a :symbol:`bson:bson_t` that is valid only for the lifetime of ``session``. Otherwise, ``NULL``.

.. only:: html

  .. include:: includes/seealso/session.txt
