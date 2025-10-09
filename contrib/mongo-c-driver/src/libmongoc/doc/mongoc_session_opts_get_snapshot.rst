:man_page: mongoc_session_opts_get_snapshot

mongoc_session_opts_get_snapshot()
==================================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_session_opts_get_snapshot (const mongoc_session_opt_t *opts);

Return true if this session is configured for snapshot reads, false by default. See :symbol:`mongoc_session_opts_set_snapshot()`.

Parameters
----------

* ``opts``: A :symbol:`mongoc_session_opt_t`.

.. only:: html

  .. include:: includes/seealso/session.txt