:man_page: mongoc_change_stream_get_resume_token

mongoc_change_stream_get_resume_token()
=======================================

Synopsis
--------

.. code-block:: c

  const bson_t *
  mongoc_change_stream_get_resume_token (mongoc_change_stream_t *stream);

This function returns the cached resume token, which may be passed as either the
``resumeAfter`` or ``startAfter`` option of a ``watch`` function to start a new
change stream from the same point.

Parameters
----------

* ``stream``: A :symbol:`mongoc_change_stream_t`.

Returns
-------

A :symbol:`bson:bson_t` that should not be modified or freed.

Returns ``NULL`` if no resume token is available. This is possible if the change
stream has not been iterated and neither ``resumeAfter`` nor ``startAfter``
options were specified in the ``watch`` function.

Lifecycle
---------

The returned :symbol:`bson:bson_t` is valid for the lifetime of ``stream`` and
its data may be updated if :symbol:`mongoc_change_stream_next` is called after
this function. The value may be copied to extend its lifetime or preserve the
current resume token.
