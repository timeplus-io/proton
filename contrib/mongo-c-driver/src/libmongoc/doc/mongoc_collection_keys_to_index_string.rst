:man_page: mongoc_collection_keys_to_index_string

mongoc_collection_keys_to_index_string()
========================================

Synopsis
--------

.. code-block:: c

  char *
  mongoc_collection_keys_to_index_string (const bson_t *keys)
     BSON_GNUC_WARN_UNUSED_RESULT;

Parameters
----------

* ``keys``: A :symbol:`bson:bson_t`. This is expected to match the form of the ``key`` field in the `createIndexes <https://www.mongodb.com/docs/manual/reference/command/createIndexes/>`_ command.

Description
-----------

This function returns the canonical stringification of a given key specification. See `Manage Collection Indexes <manage-collection-indexes_>`_ for example usage.

It is a programming error to call this function on a non-standard index, such one other than a straight index with ascending and descending.

Returns
-------

A string that should be freed with :symbol:`bson:bson_free()`.

