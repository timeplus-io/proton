:man_page: mongoc_collection_drop_with_opts

mongoc_collection_drop_with_opts()
==================================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_collection_drop_with_opts (mongoc_collection_t *collection,
                                    bson_t *opts,
                                    bson_error_t *error);

Parameters
----------

* ``collection``: A :symbol:`mongoc_collection_t`.
* ``error``: An optional location for a :symbol:`bson_error_t <errors>` or ``NULL``.

.. |opts-source| replace:: ``collection``

.. include:: includes/write-opts.txt

Description
-----------

This function requests that a ``collection`` be dropped, including all indexes associated with the ``collection``.

If no write concern is provided in ``opts``, the collection's write concern is used.

If the collection does not exist, the server responds with an "ns not found" error. It is safe to ignore this error; set the :ref:`Error API Version <error_api_version>` to 2 and ignore server error code 26:

.. code-block:: c

  mongoc_client_t *client;
  mongoc_collection_t *collection;
  bson_error_t error;
  bool r;

  client = mongoc_client_new (NULL);
  mongoc_client_set_error_api (client, 2);
  collection = mongoc_client_get_collection (client, "db", "collection");
  r = mongoc_collection_drop_with_opts (collection, NULL /* opts */, &error);
  if (r) {
     printf ("Dropped.\n");
  } else {
     printf ("Error message: %s\n", error.message);
     if (error.domain == MONGOC_ERROR_SERVER && error.code == 26) {
        printf ("Ignoring 'ns not found' error\n");
     } else {
        fprintf (stderr, "Unrecognized error!\n");
     }
  }

  mongoc_collection_destroy (collection);
  mongoc_client_destroy (client);

In MongoDB 3.0 and older, the "ns not found" error code is the generic MONGOC_ERROR_QUERY_FAILURE; in this case check whether the error message is equal to the string "ns not found".

The ``encryptedFields`` document in ``opts`` may be used to drop a collection for `Queryable Encryption <queryable-encryption_>`_. If ``encryptedFields`` is specified, the "ns not found" error is not returned.

Errors
------

Errors are propagated via the ``error`` parameter.

Returns
-------

Returns true if the collection was successfully dropped. Returns ``false`` and sets ``error`` if there are invalid arguments or a server or network error.

