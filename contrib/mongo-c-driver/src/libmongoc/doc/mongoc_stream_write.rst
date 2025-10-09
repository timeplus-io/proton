:man_page: mongoc_stream_write

mongoc_stream_write()
=====================

Synopsis
--------

.. code-block:: c

  ssize_t
  mongoc_stream_write (mongoc_stream_t *stream,
                       void *buf,
                       size_t count,
                       int32_t timeout_msec);

Parameters
----------

* ``stream``: A :symbol:`mongoc_stream_t`.
* ``buf``: The buffer to write.
* ``count``: The number of bytes to write.
* ``timeout_msec``: The number of milliseconds to wait before failure, a timeout of 0 will not block. If negative, use the default timeout.

The :symbol:`mongoc_stream_write()` function shall perform a write to a :symbol:`mongoc_stream_t`. It's modeled on the API and semantics of ``write()``, though the parameters map only loosely.

.. warning::

  The "default timeout" indicated by a negative value is both unspecified and
  unrelated to the documented default values for ``*TimeoutMS`` URI options.
  To specify a default timeout value for a ``*TimeoutMS`` URI option, use the
  ``MONGOC_DEFAULT_*`` constants defined in ``mongoc-client.h``.

Returns
-------

The :symbol:`mongoc_stream_write` function returns the number of bytes written on success. It returns ``-1`` and sets ``errno`` upon failure.

.. seealso::

  | :symbol:`mongoc_stream_read()`

  | :symbol:`mongoc_stream_readv()`

  | :symbol:`mongoc_stream_writev()`
