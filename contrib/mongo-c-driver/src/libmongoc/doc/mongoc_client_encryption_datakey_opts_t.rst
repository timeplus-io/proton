:man_page: mongoc_client_encryption_datakey_opts_t

mongoc_client_encryption_datakey_opts_t
=======================================

Synopsis
--------

.. code-block:: c

  typedef struct _mongoc_client_encryption_datakey_opts_t mongoc_client_encryption_datakey_opts_t;

Used to set options for :symbol:`mongoc_client_encryption_create_datakey()`.

.. only:: html

  Functions
  ---------

  .. toctree::
    :titlesonly:
    :maxdepth: 1

    mongoc_client_encryption_datakey_opts_new
    mongoc_client_encryption_datakey_opts_destroy
    mongoc_client_encryption_datakey_opts_set_masterkey
    mongoc_client_encryption_datakey_opts_set_keyaltnames
    mongoc_client_encryption_datakey_opts_set_keymaterial

.. seealso::

  | :symbol:`mongoc_client_encryption_create_datakey()`
