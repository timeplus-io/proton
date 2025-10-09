:man_page: mongoc_ssl_opt_t

mongoc_ssl_opt_t
================

Synopsis
--------

.. code-block:: c

  typedef struct {
     const char *pem_file;
     const char *pem_pwd;
     const char *ca_file;
     const char *ca_dir;
     const char *crl_file;
     bool weak_cert_validation;
     bool allow_invalid_hostname;
     void *internal;
     void *padding[6];
  } mongoc_ssl_opt_t;

Description
-----------

This structure is used to set the TLS options for a :symbol:`mongoc_client_t` or :symbol:`mongoc_client_pool_t`.

Beginning in version 1.2.0, once a pool or client has any TLS options set, all connections use TLS, even if ``ssl=true`` is omitted from the MongoDB URI. Before, TLS options were ignored unless ``tls=true`` was included in the URI.

As of 1.4.0, the :symbol:`mongoc_client_pool_set_ssl_opts` and :symbol:`mongoc_client_set_ssl_opts` will not only shallow copy the struct, but will also copy the ``const char*``. It is therefore no longer needed to make sure the values remain valid after setting them.

.. only:: html

  Functions
  ---------

  .. toctree::
    :titlesonly:
    :maxdepth: 1

    mongoc_ssl_opt_get_default

.. seealso::

  | `Configuring TLS <configuring_tls_>`_

  | :doc:`mongoc_client_set_ssl_opts`

  | :doc:`mongoc_client_pool_set_ssl_opts`

