:man_page: bson_iter_visit_all

bson_iter_visit_all()
=====================

Synopsis
--------

.. code-block:: c

  bool
  bson_iter_visit_all (bson_iter_t *iter,
                       const bson_visitor_t *visitor,
                       void *data);

Parameters
----------

* ``iter``: A :symbol:`bson_iter_t`.
* ``visitor``: A :symbol:`bson_visitor_t`.
* ``data``: Optional data for ``visitor``.

Description
-----------

A convenience function to iterate all remaining fields of ``iter`` using the callback vtable provided by ``visitor``.

Returns
-------

Returns true if visitation was prematurely stopped by a callback function. Returns false either because all elements were visited *or* due to corrupt BSON.

See :symbol:`bson_visitor_t` for examples of how to set your own callbacks to provide information about the location of corrupt or unsupported BSON document entries.

