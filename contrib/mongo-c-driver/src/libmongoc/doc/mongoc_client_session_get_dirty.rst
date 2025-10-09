:man_page: mongoc_client_session_get_dirty

mongoc_client_session_get_dirty()
=================================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_client_session_get_dirty (const mongoc_client_session_t *session);

Indicates whether ``session`` has been marked "dirty" as defined in the `driver sessions specification <https://github.com/mongodb/specifications/blob/master/source/sessions/driver-sessions.rst>`_.

Parameters
----------

* ``session``: A const :symbol:`mongoc_client_session_t`.

Description
-----------

This function is intended for use by drivers that wrap libmongoc. It is not useful in client applications.

Returns
-------

A boolean indicating whether the session has been marked "dirty".

.. only:: html

  .. include:: includes/seealso/session.txt
