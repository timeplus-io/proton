:man_page: mongoc_client_write_command_with_opts

mongoc_client_write_command_with_opts()
=======================================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_client_write_command_with_opts (mongoc_client_t *client,
                                         const char *db_name,
                                         const bson_t *command,
                                         const bson_t *opts,
                                         bson_t *reply,
                                         bson_error_t *error);

Execute a command on the server, applying logic that is specific to commands that write, and taking the MongoDB server version into account. To send a raw command to the server without any of this logic, use :symbol:`mongoc_client_command_simple`.

.. |opts-source| replace:: ``client``

.. include:: includes/write-opts-sources.txt

``reply`` is always initialized, and must be freed with :symbol:`bson:bson_destroy()`.

Parameters
----------

* ``client``: A :symbol:`mongoc_client_t`.
* ``db_name``: The name of the database to run the command on.
* ``command``: A :symbol:`bson:bson_t` containing the command specification.
* ``opts``: A :symbol:`bson:bson_t` containing additional options.
* ``reply``: A |bson_t-opt-storage-ptr| to contain the results.
* ``error``: An optional location for a :symbol:`bson_error_t <errors>` or ``NULL``.

.. include:: includes/write-opts.txt

Consult `the MongoDB Manual entry on Database Commands <https://www.mongodb.com/docs/manual/reference/command/>`_ for each command's arguments.

Errors
------

Errors are propagated via the ``error`` parameter.

Returns
-------

Returns ``true`` if successful. Returns ``false`` and sets ``error`` if there are invalid arguments or a server or network error.

A write concern timeout or write concern error is considered a failure.

Basic Write Operations
----------------------

Do not use this function to call the basic write commands "insert", "update", and "delete". Those commands require special logic not implemented in ``mongoc_client_write_command_with_opts``. For basic write operations use CRUD functions such as :symbol:`mongoc_collection_insert_one` and the others described in `the CRUD tutorial <tutorial_crud_operations_>`_, or use the `Bulk API <bulk_>`_.

Example
-------

.. literalinclude:: ../examples/example-command-with-opts.c
   :language: c
   :caption: example-command-with-opts.c

