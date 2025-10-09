:man_page: bson_iter_overwrite_oid

bson_iter_overwrite_oid()
=========================

Synopsis
--------

.. code-block:: c

  void
  bson_iter_overwrite_oid (bson_iter_t *iter, const bson_oid_t *value);

Parameters
----------

* ``iter``: A :symbol:`bson_iter_t`.
* ``oid``: A :symbol:`bson_oid_t`.

Description
-----------

The ``bson_iter_overwrite_oid()`` function shall overwrite the contents of a BSON_TYPE_OID element in place.

This may only be done when the underlying bson document allows mutation.

It is a programming error to call this function when ``iter`` is not observing an element of type BSON_TYPE_OID.

