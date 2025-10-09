:man_page: mongoc_stream_should_retry

mongoc_stream_should_retry()
============================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_stream_should_retry (mongoc_stream_t *stream);

Parameters
----------

* ``stream``: A :symbol:`mongoc_stream_t`.

Returns
-------

True if the stream is open and has encountered a retryable network error such as EAGAIN or if a TLS exchange is in progress and needs more data.
