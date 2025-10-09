:man_page: mongoc_stream_timed_out

mongoc_stream_timed_out()
=========================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_stream_timed_out (mongoc_stream_t *stream);

Parameters
----------

* ``stream``: A :symbol:`mongoc_stream_t`.

Returns
-------

True if there has been a network timeout error on this stream.
