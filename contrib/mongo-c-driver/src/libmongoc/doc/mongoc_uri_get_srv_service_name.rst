:man_page: mongoc_uri_get_srv_service_name

mongoc_uri_get_srv_service_name()
=================================

Synopsis
--------

.. code-block:: c

  const char *
  mongoc_uri_get_srv_service_name (const mongoc_uri_t *uri);

Parameters
----------

* ``uri``: A :symbol:`mongoc_uri_t`.

Description
-----------

Returns the SRV service name of a MongoDB URI.

Returns
-------

A string corresponding to the value of the srvServiceName URI option if present. Otherwise, the default value "mongodb".
