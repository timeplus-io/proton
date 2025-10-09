:man_page: mongoc_session_opts_clone

mongoc_session_opts_clone()
===========================

Synopsis
--------

.. code-block:: c

  mongoc_session_opt_t *
  mongoc_session_opts_clone (const mongoc_session_opt_t *opts)
     BSON_GNUC_WARN_UNUSED_RESULT;

Create a copy of a session options struct.

Parameters
----------

* ``opts``: A :symbol:`mongoc_session_opt_t`.

Returns
-------

A new :symbol:`mongoc_session_opt_t` that must be freed with :symbol:`mongoc_session_opts_destroy()`.

.. only:: html

  .. include:: includes/seealso/session.txt
