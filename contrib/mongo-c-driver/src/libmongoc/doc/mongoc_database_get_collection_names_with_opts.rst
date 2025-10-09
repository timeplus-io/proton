:man_page: mongoc_database_get_collection_names_with_opts

mongoc_database_get_collection_names_with_opts()
================================================

Synopsis
--------

.. code-block:: c

  char **
  mongoc_database_get_collection_names_with_opts (mongoc_database_t *database,
                                                  const bson_t *opts,
                                                  bson_error_t *error)
     BSON_GNUC_WARN_UNUSED_RESULT;

Fetches a ``NULL`` terminated array of ``NULL-byte`` terminated ``char*`` strings containing the names of all of the collections in ``database``.

.. include:: includes/retryable-read.txt

Parameters
----------

* ``database``: A :symbol:`mongoc_database_t`.
* ``opts``: A :symbol:`bson:bson_t` containing additional options.
* ``error``: An optional location for a :symbol:`bson_error_t <errors>` or ``NULL``.

.. |opts-source| replace:: ``database``

.. include:: includes/generic-opts.txt

For a list of all options, see `the MongoDB Manual entry on the listCollections command <https://www.mongodb.com/docs/manual/reference/command/listCollections/>`_.

Errors
------

Errors are propagated via the ``error`` parameter.

Returns
-------

A ``NULL`` terminated array of ``NULL`` terminated ``char*`` strings that should be freed with :symbol:`bson:bson_strfreev()`. Upon failure, ``NULL`` is returned and ``error`` is set.

Examples
--------

.. code-block:: c

  {
     bson_t opts = BSON_INITIALIZER;
     mongoc_read_concern_t *rc;
     bson_error_t error;
     char **strv;
     unsigned i;

     rc = mongoc_read_concern_new ();
     mongoc_read_concern_set_level (rc, MONGOC_READ_CONCERN_LEVEL_MAJORITY);
     mongoc_read_concern_append (rc, &opts);
     if ((strv = mongoc_database_get_collection_names_with_opts (
            database, &opts, &error))) {

        for (i = 0; strv[i]; i++)
           printf ("%s\n", strv[i]);

        bson_strfreev (strv);
     } else {
        fprintf (stderr, "Command failed: %s\n", error.message);
     }

     mongoc_read_concern_destroy (rc);
     bson_destroy (&opts);
  }
