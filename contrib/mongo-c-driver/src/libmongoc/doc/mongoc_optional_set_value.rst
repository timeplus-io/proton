:man_page: mongoc_optional_set_value

mongoc_optional_set_value()
===========================

Synopsis
--------

.. code-block:: c

  void
  mongoc_optional_set_value (mongoc_optional_t *opt);

Sets a value on a :symbol:`mongoc_optional_t`. Once a value has been set, it cannot be unset, i.e. `mongoc_optional_is_set` will always return ``true`` after calling `mongoc_optional_set_value`.

Parameters
----------

* ``opt``: A :symbol:`mongoc_optional_t`.
