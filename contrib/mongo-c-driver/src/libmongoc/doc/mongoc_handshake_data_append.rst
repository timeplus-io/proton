:man_page: mongoc_handshake_data_append

mongoc_handshake_data_append()
==============================

Synopsis
--------

.. code-block:: c

   bool
   mongoc_handshake_data_append (const char *driver_name,
                                 const char *driver_version,
                                 const char *platform);

Appends the given strings to the handshake data for the underlying C Driver.

Description
-----------

This function is intended for use by drivers which wrap the C Driver.
Calling this function will store the given strings as handshake data about
the system and driver by appending them to the handshake data for the
underlying C Driver. These values, along with other handshake data collected
during mongoc_init will be sent to the server as part of the initial
connection handshake in the "client" document. This function must not be
called more than once, or after server monitoring begins. For a single-threaded 
:symbol:`mongoc_client_t`, server monitoring begins on the first operation 
requiring a server. For a :symbol:`mongoc_client_pool_t`, server monitoring 
begins on the first call to `:symbol:`mongoc_client_pool_pop`.

The passed in strings are copied, and don't have to remain valid after the
call to :symbol:`mongoc_handshake_data_append`. The driver may store truncated
versions of the passed in strings.

.. note::
  This function allocates memory, and therefore caution should be used when
  using this in conjunction with :symbol:`bson_mem_set_vtable`. If this function is
  called before :symbol:`bson_mem_set_vtable`, then :symbol:`bson_mem_restore_vtable` must be
  called before calling :symbol:`mongoc_cleanup`. Failure to do so will result in
  memory being freed by the wrong allocator.

Parameters
----------

* ``driver_name``: An optional string storing the name of the wrapping driver
* ``driver_version``: An optional string storing the version of the wrapping driver.
* ``platform``: An optional string storing any information about the current platform, for example configure options or compile flags.

Returns
-------

``true`` if the given fields are set successfully. Otherwise, it returns ``false`` and logs an error.

The default handshake data the driver sends with "hello" looks something
like:

.. code-block:: c

 client: {
   driver: {
     name: "mongoc",
     version: "1.5.0"
   },
   os: {...},
   platform: "CC=gcc CFLAGS=-Wall -pedantic"
 }

If we call
:symbol:`mongoc_handshake_data_append` ("phongo", "1.1.8", "CC=clang / ")
and it returns true, the driver sends handshake data like:

.. code-block:: c

 client: {
   driver: {
     name: "mongoc / phongo",
     version: "1.5.0 / 1.1.8"
   },
   os: {...},
   platform: "CC=clang / gcc CFLAGS=-Wall -pedantic"
 }


