:man_page: mongoc_session_opts_get_causal_consistency

mongoc_session_opts_get_causal_consistency()
============================================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_session_opts_get_causal_consistency (const mongoc_session_opt_t *opts);

Return true if this session is configured for causal consistency (the default), else false. See :symbol:`mongoc_session_opts_set_causal_consistency()`.

Parameters
----------

* ``opts``: A :symbol:`mongoc_session_opt_t`.

.. only:: html

  .. include:: includes/seealso/session.txt
