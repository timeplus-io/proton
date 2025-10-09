:man_page: mongoc_uri_get_tls

mongoc_uri_get_tls()
====================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_uri_get_tls (const mongoc_uri_t *uri);

Parameters
----------

* ``uri``: A :symbol:`mongoc_uri_t`.

Description
-----------

Fetches a boolean indicating if TLS was specified for use in the URI.

Returns
-------

Returns a boolean, true indicating that TLS should be used. This returns true if *any* :ref:`TLS option <tls_options>` is specified.

