:man_page: mongoc_server_description_t

mongoc_server_description_t
===========================

Server description

Synopsis
--------

.. code-block:: c

  #include <mongoc/mongoc.h>
  typedef struct _mongoc_server_description_t mongoc_server_description_t

``mongoc_server_description_t`` holds information about a mongod or mongos the driver is connected to.

Lifecycle
---------

Clean up a ``mongoc_server_description_t`` with :symbol:`mongoc_server_description_destroy()` when necessary.

Applications receive a temporary reference to a ``mongoc_server_description_t`` as a parameter to an SDAM Monitoring callback that must not be destroyed. See
:doc:`Introduction to Application Performance Monitoring <application-performance-monitoring>`.

.. only:: html

  Functions
  ---------

  .. toctree::
    :titlesonly:
    :maxdepth: 1

    mongoc_server_description_destroy
    mongoc_server_description_hello_response
    mongoc_server_description_host
    mongoc_server_description_id
    mongoc_server_description_ismaster
    mongoc_server_description_last_update_time
    mongoc_server_description_new_copy
    mongoc_server_description_round_trip_time
    mongoc_server_description_type
    mongoc_server_descriptions_destroy_all

.. seealso::

  | :symbol:`mongoc_client_get_server_descriptions()`.

