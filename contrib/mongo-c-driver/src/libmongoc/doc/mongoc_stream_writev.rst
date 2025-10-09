:man_page: mongoc_stream_writev

mongoc_stream_writev()
======================

Synopsis
--------

.. code-block:: c

  ssize_t
  mongoc_stream_writev (mongoc_stream_t *stream,
                        mongoc_iovec_t *iov,
                        size_t iovcnt,
                        int32_t timeout_msec);

Parameters
----------

* ``stream``: A :symbol:`mongoc_stream_t`.
* ``iov``: A vector of :symbol:`mongoc_iovec_t`.
* ``iovcnt``: The number of items in ``iov``.
* ``timeout_msec``: The number of milliseconds to block before indicating failure, or 0 for non-blocking. Negative values indicate the default timeout.

The ``mongoc_stream_writev()`` function shall perform a write
to a :symbol:`mongoc_stream_t`. It's modeled on the
API and semantics of ``writev()``, though the parameters map only
loosely.

.. warning::

  The "default timeout" indicated by a negative value is both unspecified and
  unrelated to the documented default values for ``*TimeoutMS`` URI options.
  To specify a default timeout value for a ``*TimeoutMS`` URI option, use the
  ``MONGOC_DEFAULT_*`` constants defined in ``mongoc-client.h``.

Returns
-------

The number of bytes written on success, or ``-1`` upon failure and ``errno`` is set.
