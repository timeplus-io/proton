:man_page: mongoc_find_and_modify_opts_t

mongoc_find_and_modify_opts_t
=============================

find_and_modify abstraction

Synopsis
--------

``mongoc_find_and_modify_opts_t`` is a builder interface to construct `the findAndModify command`_.

It was created to be able to accommodate new arguments to `the findAndModify command`_.

As of MongoDB 3.2, the :symbol:`mongoc_write_concern_t` specified on the :symbol:`mongoc_collection_t` will be used, if any.

.. _mongoc_collection_find_and_modify_with_opts_example:

Example
-------

.. literalinclude:: ../examples/find_and_modify_with_opts/fam.c
   :language: c
   :start-after: /* EXAMPLE_FAM_FLAGS_BEGIN */
   :end-before: /* EXAMPLE_FAM_FLAGS_END */
   :caption: flags.c

.. literalinclude:: ../examples/find_and_modify_with_opts/fam.c
   :language: c
   :start-after: /* EXAMPLE_FAM_BYPASS_BEGIN */
   :end-before: /* EXAMPLE_FAM_BYPASS_END */
   :caption: bypass.c

.. literalinclude:: ../examples/find_and_modify_with_opts/fam.c
   :language: c
   :start-after: /* EXAMPLE_FAM_UPDATE_BEGIN */
   :end-before: /* EXAMPLE_FAM_UPDATE_END */
   :caption: update.c

.. literalinclude:: ../examples/find_and_modify_with_opts/fam.c
   :language: c
   :start-after: /* EXAMPLE_FAM_FIELDS_BEGIN */
   :end-before: /* EXAMPLE_FAM_FIELDS_END */
   :caption: fields.c

.. literalinclude:: ../examples/find_and_modify_with_opts/fam.c
   :language: c
   :start-after: /* EXAMPLE_FAM_SORT_BEGIN */
   :end-before: /* EXAMPLE_FAM_SORT_END */
   :caption: sort.c

.. literalinclude:: ../examples/find_and_modify_with_opts/fam.c
   :language: c
   :start-after: /* EXAMPLE_FAM_OPTS_BEGIN */
   :end-before: /* EXAMPLE_FAM_OPTS_END */
   :caption: opts.c

.. literalinclude:: ../examples/find_and_modify_with_opts/fam.c
   :language: c
   :start-after: /* EXAMPLE_FAM_MAIN_BEGIN */
   :end-before: /* EXAMPLE_FAM_MAIN_END */
   :caption: fam.c

Outputs:

.. code-block:: json

  {
      "lastErrorObject": {
          "updatedExisting": false,
          "n": 1,
          "upserted": {
              "$oid": "56562a99d13e6d86239c7b00"
          }
      },
      "value": {
          "_id": {
              "$oid": "56562a99d13e6d86239c7b00"
          },
          "age": 34,
          "firstname": "Zlatan",
          "goals": 342,
          "lastname": "Ibrahimovic",
          "profession": "Football player",
          "position": "striker"
      },
      "ok": 1
  }
  {
      "lastErrorObject": {
          "updatedExisting": true,
          "n": 1
      },
      "value": {
          "_id": {
              "$oid": "56562a99d13e6d86239c7b00"
          },
          "age": 34,
          "firstname": "Zlatan",
          "goals": 342,
          "lastname": "Ibrahimovic",
          "profession": "Football player",
          "position": "striker"
      },
      "ok": 1
  }
  {
      "lastErrorObject": {
          "updatedExisting": true,
          "n": 1
      },
      "value": {
          "_id": {
              "$oid": "56562a99d13e6d86239c7b00"
          },
          "age": 35,
          "firstname": "Zlatan",
          "goals": 342,
          "lastname": "Ibrahimovic",
          "profession": "Football player",
          "position": "striker"
      },
      "ok": 1
  }
  {
      "lastErrorObject": {
          "updatedExisting": true,
          "n": 1
      },
      "value": {
          "_id": {
              "$oid": "56562a99d13e6d86239c7b00"
          },
          "goals": 343
      },
      "ok": 1
  }
  {
      "lastErrorObject": {
          "updatedExisting": true,
          "n": 1
      },
      "value": {
          "_id": {
              "$oid": "56562a99d13e6d86239c7b00"
          },
          "age": 35,
          "firstname": "Zlatan",
          "goals": 343,
          "lastname": "Ibrahimovic",
          "profession": "Football player",
          "position": "striker",
          "author": true
      },
      "ok": 1
  }

.. only:: html

  Functions
  ---------

  .. toctree::
    :titlesonly:
    :maxdepth: 1

    mongoc_find_and_modify_opts_append
    mongoc_find_and_modify_opts_destroy
    mongoc_find_and_modify_opts_get_bypass_document_validation
    mongoc_find_and_modify_opts_get_fields
    mongoc_find_and_modify_opts_get_flags
    mongoc_find_and_modify_opts_get_max_time_ms
    mongoc_find_and_modify_opts_get_sort
    mongoc_find_and_modify_opts_get_update
    mongoc_find_and_modify_opts_new
    mongoc_find_and_modify_opts_set_bypass_document_validation
    mongoc_find_and_modify_opts_set_fields
    mongoc_find_and_modify_opts_set_flags
    mongoc_find_and_modify_opts_set_max_time_ms
    mongoc_find_and_modify_opts_set_sort
    mongoc_find_and_modify_opts_set_update

