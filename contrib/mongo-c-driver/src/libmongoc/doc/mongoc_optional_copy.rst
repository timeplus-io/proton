:man_page: mongoc_optional_copy

mongoc_optional_copy()
======================

Synopsis
--------

.. code-block:: c

  void
  mongoc_optional_copy (const mongoc_optional_t *source, mongoc_optional_t *copy);

Creates a deep copy of ``source`` into ``copy``.

Parameters
----------

* ``source``: A :symbol:`mongoc_optional_t`.
* ``copy``: An empty :symbol:`mongoc_optional_t`. Values will be overwritten.
