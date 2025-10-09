:man_page: mongoc_change_stream_destroy

mongoc_change_stream_destroy()
==============================

Synopsis
--------

.. code-block:: c

  void
  mongoc_change_stream_destroy (mongoc_change_stream_t *stream);

Destroys a change stream and associated data.

Parameters
----------

* ``stream``: An allocated :symbol:`mongoc_change_stream_t`.