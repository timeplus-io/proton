:man_page: mongoc_client_session_commit_transaction

mongoc_client_session_commit_transaction()
==========================================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_client_session_commit_transaction (mongoc_client_session_t *session,
                                            bson_t *reply,
                                            bson_error_t *error);


Commit a multi-document transaction.

Parameters
----------

* ``session``: A :symbol:`mongoc_client_session_t`.
* ``reply``: A |bson_t-opt-storage-ptr| to contain the results.
* ``error``: An optional location for a :symbol:`bson_error_t <errors>` or ``NULL``.

Return
------

Returns true if the transaction was committed. Returns ``false`` and sets ``error`` if there are invalid arguments, such as a session with no transaction in progress, or if there is a server or network error.

If a ``reply`` is supplied, it is always initialized and must be freed with :symbol:`bson:bson_destroy`.

.. only:: html

  .. include:: includes/seealso/session.txt
