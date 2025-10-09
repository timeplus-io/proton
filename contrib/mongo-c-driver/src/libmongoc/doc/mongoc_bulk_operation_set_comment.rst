:man_page: mongoc_bulk_operation_set_comment

mongoc_bulk_operation_set_comment()
===================================

Synopsis
--------

.. code-block:: c

  void
  mongoc_bulk_operation_set_comment (
     mongoc_bulk_operation_t *bulk, const bson_value_t *comment);

Parameters
----------

* ``bulk``: A :symbol:`mongoc_bulk_operation_t`.
* ``comment``: A :symbol:`bson_value_t` specifying the comment to associate with this bulk write.

Description
-----------

Assigns a comment to attach to all commands executed as part of this :doc:`bulk <mongoc_bulk_operation_t>`. The comment will appear in log messages, profiler output, and currentOp output. Comments for write commands are only supported by MongoDB 4.4+.

It is prohibited to call this function after adding operations to the :doc:`bulk <mongoc_bulk_operation_t>`.
