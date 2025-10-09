:man_page: mongoc_uri_get_ssl

mongoc_uri_get_ssl()
====================

.. warning::
   .. deprecated:: 1.15.0

      This function is deprecated and should not be used in new code. See `Manage Collection Indexes <manage-collection-indexes_>`_.


This function is deprecated and should not be used in new code.

Please use :doc:`mongoc_uri_get_tls() <mongoc_uri_get_tls>` in new code.

Synopsis
--------

.. code-block:: c

  bool
  mongoc_uri_get_ssl (const mongoc_uri_t *uri)
     BSON_GNUC_DEPRECATED_FOR (mongoc_uri_get_tls);

Parameters
----------

* ``uri``: A :symbol:`mongoc_uri_t`.

Description
-----------

Fetches a boolean indicating if TLS was specified for use in the URI.

Returns
-------

Returns a boolean, true indicating that TLS should be used. This returns true if *any* :ref:`TLS option <tls_options>` is specified.

