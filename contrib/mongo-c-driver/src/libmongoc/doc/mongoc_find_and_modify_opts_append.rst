:man_page: mongoc_find_and_modify_opts_append

mongoc_find_and_modify_opts_append()
====================================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_find_and_modify_opts_append (mongoc_find_and_modify_opts_t *opts,
                                      const bson_t *extra);

Parameters
----------

* ``opts``: A :symbol:`mongoc_find_and_modify_opts_t`.
* ``extra``: A :symbol:`bson:bson_t` with fields and values to append directly to `the findAndModify command`_ sent to the server.

Description
-----------

Adds arbitrary options to `the findAndModify command`_.

``extra`` does not have to remain valid after calling this function.

.. |opts-source| replace:: ``collection``

.. include:: includes/find-and-modify-appended-opts.txt

Returns
-------

Returns true on success. If any arguments are invalid, returns false and logs an error.

Appending options to findAndModify
----------------------------------

.. literalinclude:: ../examples/find_and_modify_with_opts/fam.c
   :language: c
   :start-after: /* EXAMPLE_FAM_OPTS_BEGIN */
   :end-before: /* EXAMPLE_FAM_OPTS_END */
   :caption: opts.c

