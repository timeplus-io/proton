:man_page: mongoc_change_stream_next

mongoc_change_stream_next()
===========================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_change_stream_next (mongoc_change_stream_t *stream,
                             const bson_t **bson);

This function iterates the underlying cursor, setting ``bson`` to the next
document. This will block for a maximum of ``maxAwaitTimeMS`` milliseconds as
specified in the options when created, or the default timeout if omitted. Data
may be returned before the timeout. If no data is returned this function returns
``false``.

Parameters
----------

* ``stream``: A :symbol:`mongoc_change_stream_t`.
* ``bson``: The location for the resulting document.

Returns
-------

This function returns true if a valid bson document was read from the stream.
Otherwise, false if there was an error or no document was available.

Errors can be determined with the :symbol:`mongoc_change_stream_error_document`
function.

Lifecycle
---------

Similar to :symbol:`mongoc_cursor_next` the lifetime of ``bson`` is until the
next call to :symbol:`mongoc_change_stream_next`, so it needs to be copied to
extend the lifetime.
