:man_page: mongoc_session_opts_destroy

mongoc_session_opts_destroy()
=============================

Synopsis
--------

.. code-block:: c

  void
  mongoc_session_opts_destroy (mongoc_session_opt_t *opts);

Free a :symbol:`mongoc_session_opt_t`. Does nothing if ``opts`` is NULL.

Parameters
----------

* ``opts``: A :symbol:`mongoc_session_opt_t`.

Example
-------

See the example code for :symbol:`mongoc_session_opts_set_causal_consistency`.

.. only:: html

  .. include:: includes/seealso/session.txt
