:man_page: mongoc_client_session_get_opts

mongoc_client_session_get_opts()
================================

Synopsis
--------

.. code-block:: c

  const mongoc_session_opt_t *
  mongoc_client_session_get_opts (const mongoc_client_session_t *session);

Get a reference to the :symbol:`mongoc_session_opt_t` with which this session was configured.

Parameters
----------

* ``session``: A :symbol:`mongoc_client_session_t`.

Returns
-------

A :symbol:`mongoc_session_opt_t` that is valid only for the lifetime of ``session``.

.. only:: html

  .. include:: includes/seealso/session.txt
