:man_page: mongoc_optional_value

mongoc_optional_value()
=======================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_optional_value (const mongoc_optional_t *opt);

Returns the value for a :symbol:`mongoc_optional_t`.

Parameters
----------

* ``opt``: A :symbol:`mongoc_optional_t`.

Returns
-------

Returns the value that was set on the :symbol:`mongoc_optional_t`. If no value was set, ``false`` is returned.
