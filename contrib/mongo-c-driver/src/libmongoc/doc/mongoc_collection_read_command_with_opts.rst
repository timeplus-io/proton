:man_page: mongoc_collection_read_command_with_opts

mongoc_collection_read_command_with_opts()
==========================================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_collection_read_command_with_opts (mongoc_collection_t *collection,
                                            const bson_t *command,
                                            const mongoc_read_prefs_t *read_prefs,
                                            const bson_t *opts,
                                            bson_t *reply,
                                            bson_error_t *error);

Execute a command on the server, applying logic that is specific to commands that read, and taking the MongoDB server version into account. To send a raw command to the server without any of this logic, use :symbol:`mongoc_collection_command_simple`.

.. |opts-source| replace:: ``collection``

.. include:: includes/read-cmd-opts-sources.txt

``reply`` is always initialized, and must be freed with :symbol:`bson:bson_destroy()`.

.. include:: includes/retryable-read.txt
.. |generic-cmd| replace:: :symbol:`mongoc_collection_command_with_opts`
.. include:: includes/retryable-read-command.txt

Parameters
----------

* ``collection``: A :symbol:`mongoc_collection_t`.
* ``command``: A :symbol:`bson:bson_t` containing the command specification.
* ``read_prefs``: An optional :symbol:`mongoc_read_prefs_t`.
* ``opts``: A :symbol:`bson:bson_t` containing additional options.
* ``reply``: A |bson_t-opt-storage-ptr| to contain the results.
* ``error``: An optional location for a :symbol:`bson_error_t <errors>` or ``NULL``.

.. include:: includes/read-opts.txt

Consult `the MongoDB Manual entry on Database Commands <https://www.mongodb.com/docs/manual/reference/command/>`_ for each command's arguments.

Errors
------

Errors are propagated via the ``error`` parameter.

Returns
-------

Returns ``true`` if successful. Returns ``false`` and sets ``error`` if there are invalid arguments or a server or network error.

Example
-------

See the example code for :symbol:`mongoc_client_read_command_with_opts`.

