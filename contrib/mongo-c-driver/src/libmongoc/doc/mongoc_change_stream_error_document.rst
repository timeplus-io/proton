:man_page: mongoc_change_stream_error_document

mongoc_change_stream_error_document()
=====================================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_change_stream_error_document (mongoc_change_stream_t *stream,
                                       bson_error_t *err,
                                       const bson_t **reply);

Checks if an error has occurred when creating or iterating over a change stream.

Similar to :symbol:`mongoc_cursor_error_document` if the error has occurred
client-side then the ``reply`` will be set to an empty BSON document. If the
error occurred server-side, ``reply`` is set to the server's reply document.

Parameters
----------

* ``stream``: A :symbol:`mongoc_change_stream_t`.
* ``err``: An optional location for a :symbol:`bson_error_t <errors>` or ``NULL``.
* ``reply``: A location for a :symbol:`bson:bson_t`.

Returns
-------
A boolean indicating if there was an error.
