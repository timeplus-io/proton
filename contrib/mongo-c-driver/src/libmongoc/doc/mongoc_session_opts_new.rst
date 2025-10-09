:man_page: mongoc_session_opts_new

mongoc_session_opts_new()
=========================

Synopsis
--------

.. code-block:: c

  mongoc_session_opt_t *
  mongoc_session_opts_new (void);

.. include:: includes/session-lifecycle.txt

See the example code for :symbol:`mongoc_session_opts_set_causal_consistency`.

Returns
-------

A new :symbol:`mongoc_session_opt_t` that must be freed with :symbol:`mongoc_session_opts_destroy()`.

.. only:: html

  .. include:: includes/seealso/session.txt
