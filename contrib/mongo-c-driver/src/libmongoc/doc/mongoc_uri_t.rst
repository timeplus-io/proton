:man_page: mongoc_uri_t

mongoc_uri_t
============

Synopsis
--------

.. code-block:: c

  typedef struct _mongoc_uri_t mongoc_uri_t;

Description
-----------

``mongoc_uri_t`` provides an abstraction on top of the MongoDB connection URI format. It provides standardized parsing as well as convenience methods for extracting useful information such as replica hosts or authorization information.

See `Connection String URI Reference <https://www.mongodb.com/docs/manual/reference/connection-string/>`_ on the MongoDB website for more information.

Format
------

.. code-block:: none

  mongodb[+srv]://                             <1>
     [username:password@]                      <2>
     host1                                     <3>
     [:port1]                                  <4>
     [,host2[:port2],...[,hostN[:portN]]]      <5>
     [/[database]                              <6>
     [?options]]                               <7>

#. "mongodb" is the specifier of the MongoDB protocol. Use "mongodb+srv" with a single service name in place of "host1" to specify the initial list of servers with an SRV record.
#. An optional username and password.
#. The only required part of the uri.  This specifies either a hostname, IPv4 address, IPv6 address enclosed in "[" and "]", or UNIX domain socket.
#. An optional port number.  Defaults to :27017.
#. Extra optional hosts and ports.  You would specify multiple hosts, for example, for connections to replica sets.
#. The name of the database to authenticate if the connection string includes authentication credentials.  If /database is not specified and the connection string includes credentials, defaults to the 'admin' database.
#. Connection specific options.

.. note::

  Option names are case-insensitive. Do not repeat the same option (e.g. "mongodb://localhost/db?opt=value1&OPT=value2") since this may have unexpected results.

The MongoDB C Driver exposes constants for each supported connection option. These constants make it easier to discover connection options, but their string values can be used as well.

For example, the following calls are equal.

.. code-block:: c

  uri = mongoc_uri_new ("mongodb://localhost/?" MONGOC_URI_APPNAME "=applicationName");
  uri = mongoc_uri_new ("mongodb://localhost/?appname=applicationName");
  uri = mongoc_uri_new ("mongodb://localhost/?appName=applicationName");

Replica Set Example
-------------------

To describe a connection to a replica set named 'test' with the following mongod hosts:

* ``db1.example.com`` on port ``27017``
* ``db2.example.com`` on port ``2500``

You would use a connection string that resembles the following.

.. code-block:: none

  mongodb://db1.example.com,db2.example.com:2500/?replicaSet=test

SRV Example
-----------

If you have configured an `SRV record <https://www.ietf.org/rfc/rfc2782.txt>`_ with a name like "_mongodb._tcp.server.example.com" whose records are a list of one or more MongoDB server hostnames, use a connection string like this:

.. code-block:: c

  uri = mongoc_uri_new ("mongodb+srv://server.example.com/?replicaSet=rs&appName=applicationName");

The driver prefixes the service name with "_mongodb._tcp.", then performs a DNS SRV query to resolve the service name to one or more hostnames. If this query succeeds, the driver performs a DNS TXT query on the service name (without the "_mongodb._tcp" prefix) for additional URI options configured as TXT records.

On Unix, the MongoDB C Driver relies on libresolv to look up SRV and TXT records. If libresolv is unavailable, then using a "mongodb+srv" URI will cause an error. If your libresolv lacks ``res_nsearch`` then the driver will fall back to ``res_search``, which is not thread-safe.

Set the environment variable ``MONGOC_EXPERIMENTAL_SRV_PREFER_TCP`` to prefer TCP for the initial queries. The environment variable is ignored for ``res_search``. Large DNS responses over UDP may be truncated due to UDP size limitations. DNS resolvers are expected to retry over TCP if the UDP response indicates truncation. Some observed DNS environments do not set the truncation flag (TC), preventing the TCP retry. This environment variable is currently experimental and subject to change.

IPv4 and IPv6
-------------

.. include:: includes/ipv4-and-ipv6.txt

.. _connection_options:

Connection Options
------------------

========================================== ================================= ================================= ============================================================================================================================================================================================================================================
Constant                                   Key                               Default                           Description
========================================== ================================= ================================= ============================================================================================================================================================================================================================================
MONGOC_URI_RETRYREADS                      retryreads                        true                              If "true" and the server is a MongoDB 3.6+ standalone, replica set, or sharded cluster, the driver safely retries a read that failed due to a network error or replica set failover.
MONGOC_URI_RETRYWRITES                     retrywrites                       true if driver built w/ TLS       If "true" and the server is a MongoDB 3.6+ replica set or sharded cluster, the driver safely retries a write that failed due to a network error or replica set failover. Only inserts, updates of single documents, or deletes of single
                                                                                                               documents are retried.
MONGOC_URI_APPNAME                         appname                           Empty (no appname)                The client application name. This value is used by MongoDB when it logs connection information and profile information, such as slow queries.
MONGOC_URI_TLS                             tls                               Empty (not set, same as false)    {true|false}, indicating if TLS must be used. (See also :symbol:`mongoc_client_set_ssl_opts` and :symbol:`mongoc_client_pool_set_ssl_opts`.)
MONGOC_URI_COMPRESSORS                     compressors                       Empty (no compressors)            Comma separated list of compressors, if any, to use to compress the wire protocol messages. Snappy, zlib, and zstd are optional build time dependencies, and enable the "snappy", "zlib", and "zstd" values respectively.
MONGOC_URI_CONNECTTIMEOUTMS                connecttimeoutms                  10,000 ms (10 seconds)            This setting applies to new server connections. It is also used as the socket timeout for server discovery and monitoring operations.
MONGOC_URI_SOCKETTIMEOUTMS                 sockettimeoutms                   300,000 ms (5 minutes)            The time in milliseconds to attempt to send or receive on a socket before the attempt times out.
MONGOC_URI_REPLICASET                      replicaset                        Empty (no replicaset)             The name of the Replica Set that the driver should connect to.
MONGOC_URI_ZLIBCOMPRESSIONLEVEL            zlibcompressionlevel              -1                                When the MONGOC_URI_COMPRESSORS includes "zlib" this options configures the zlib compression level, when the zlib compressor is used to compress client data.
MONGOC_URI_LOADBALANCED                    loadbalanced                      false                             If true, this indicates the driver is connecting to a MongoDB cluster behind a load balancer.
MONGOC_URI_SRVMAXHOSTS                     srvmaxhosts                       0                                 If zero, the number of hosts in DNS results is unlimited. If greater than zero, the number of hosts in DNS results is limited to being less than or equal to the given value.
========================================== ================================= ================================= ============================================================================================================================================================================================================================================

.. warning::

  Setting any of the \*timeoutMS options above to either ``0`` or a negative value is discouraged due to unspecified and inconsistent behavior.
  The "default value" historically specified as a fallback for ``0`` or a negative value is NOT related to the default values for the \*timeoutMS options documented above.
  The meaning of a timeout of ``0`` or a negative value may vary depending on the operation being executed, even when specified by the same URI option.
  To specify the documented default value for a \*timeoutMS option, use the `MONGOC_DEFAULT_*` constants defined in ``mongoc-client.h`` instead.

Authentication Options
----------------------

========================================== ================================= =========================================================================================================================================================================================================================
Constant                                   Key                               Description
========================================== ================================= =========================================================================================================================================================================================================================
MONGOC_URI_AUTHMECHANISM                   authmechanism                     Specifies the mechanism to use when authenticating as the provided user. See `Authentication <authentication_>`_ for supported values.
MONGOC_URI_AUTHMECHANISMPROPERTIES         authmechanismproperties           Certain authentication mechanisms have additional options that can be configured. These options should be provided as comma separated option_key:option_value pair and provided as authMechanismProperties. Specifying the same option_key multiple times has undefined behavior.
MONGOC_URI_AUTHSOURCE                      authsource                        The authSource defines the database that should be used to authenticate to. It is unnecessary to provide this option the database name is the same as the database used in the URI.
========================================== ================================= =========================================================================================================================================================================================================================

Mechanism Properties
~~~~~~~~~~~~~~~~~~~~

========================================== ================================= =========================================================================================================================================================================================================================
Constant                                   Key                               Description
========================================== ================================= =========================================================================================================================================================================================================================
MONGOC_URI_CANONICALIZEHOSTNAME            canonicalizehostname              Use the canonical hostname of the service, rather than its configured alias, when authenticating with Cyrus-SASL Kerberos.
MONGOC_URI_GSSAPISERVICENAME               gssapiservicename                 Use alternative service name. The default is ``mongodb``.
========================================== ================================= =========================================================================================================================================================================================================================


.. _tls_options:

TLS Options
-----------

.. include:: includes/tls-options.txt

See `Configuring TLS <configuring_tls_>`_ for details about these options and about building libmongoc with TLS support.

Deprecated SSL Options
----------------------

The following options have been deprecated and may be removed from future releases of libmongoc.

========================================== ================================= =========================================== =================================
Constant                                   Key                               Deprecated For                              Key
========================================== ================================= =========================================== =================================
MONGOC_URI_SSL                             ssl                               MONGOC_URI_TLS                              tls
MONGOC_URI_SSLCLIENTCERTIFICATEKEYFILE     sslclientcertificatekeyfile       MONGOC_URI_TLSCERTIFICATEKEYFILE            tlscertificatekeyfile
MONGOC_URI_SSLCLIENTCERTIFICATEKEYPASSWORD sslclientcertificatekeypassword   MONGOC_URI_TLSCERTIFICATEKEYFILEPASSWORD    tlscertificatekeypassword
MONGOC_URI_SSLCERTIFICATEAUTHORITYFILE     sslcertificateauthorityfile       MONGOC_URI_TLSCAFILE                        tlscafile
MONGOC_URI_SSLALLOWINVALIDCERTIFICATES     sslallowinvalidcertificates       MONGOC_URI_TLSALLOWINVALIDCERTIFICATES      tlsallowinvalidcertificates
MONGOC_URI_SSLALLOWINVALIDHOSTNAMES        sslallowinvalidhostnames          MONGOC_URI_TLSALLOWINVALIDHOSTNAMES         tlsallowinvalidhostnames
========================================== ================================= =========================================== =================================


.. _sdam_uri_options:

Server Discovery, Monitoring, and Selection Options
---------------------------------------------------

Clients in a :symbol:`mongoc_client_pool_t` share a topology scanner that runs on a background thread. The thread wakes every ``heartbeatFrequencyMS`` (default 10 seconds) to scan all MongoDB servers in parallel. Whenever an application operation requires a server that is not known--for example, if there is no known primary and your application attempts an insert--the thread rescans all servers every half-second. In this situation the pooled client waits up to ``serverSelectionTimeoutMS`` (default 30 seconds) for the thread to find a server suitable for the operation, then returns an error with domain ``MONGOC_ERROR_SERVER_SELECTION``.

Technically, the total time an operation may wait while a pooled client scans the topology is controlled both by ``serverSelectionTimeoutMS`` and ``connectTimeoutMS``. The longest wait occurs if the last scan begins just at the end of the selection timeout, and a slow or down server requires the full connection timeout before the client gives up.

A non-pooled client is single-threaded. Every ``heartbeatFrequencyMS``, it blocks the next application operation while it does a parallel scan. This scan takes as long as needed to check the slowest server: roughly ``connectTimeoutMS``. Therefore the default ``heartbeatFrequencyMS`` for single-threaded clients is greater than for pooled clients: 60 seconds.

By default, single-threaded (non-pooled) clients scan only once when an operation requires a server that is not known. If you attempt an insert and there is no known primary, the client checks all servers once trying to find it, then succeeds or returns an error with domain ``MONGOC_ERROR_SERVER_SELECTION``. But if you set ``serverSelectionTryOnce`` to "false", the single-threaded client loops, checking all servers every half-second, until ``serverSelectionTimeoutMS``.

The total time an operation may wait for a single-threaded client to scan the topology is determined by ``connectTimeoutMS`` in the try-once case, or ``serverSelectionTimeoutMS`` and ``connectTimeoutMS`` if ``serverSelectionTryOnce`` is set "false".

========================================== ================================= =========================================================================================================================================================================================================================
Constant                                   Key                               Description
========================================== ================================= =========================================================================================================================================================================================================================
MONGOC_URI_HEARTBEATFREQUENCYMS            heartbeatfrequencyms              The interval between server monitoring checks. Defaults to 10,000ms (10 seconds) in pooled (multi-threaded) mode, 60,000ms (60 seconds) in non-pooled mode (single-threaded).
MONGOC_URI_SERVERSELECTIONTIMEOUTMS        serverselectiontimeoutms          A timeout in milliseconds to block for server selection before throwing an exception. The default is 30,0000ms (30 seconds).
MONGOC_URI_SERVERSELECTIONTRYONCE          serverselectiontryonce            If "true", the driver scans the topology exactly once after server selection fails, then either selects a server or returns an error. If it is false, then the driver repeatedly searches for a suitable server for up to ``serverSelectionTimeoutMS`` milliseconds (pausing a half second between attempts). The default for ``serverSelectionTryOnce`` is "false" for pooled clients, otherwise "true". Pooled clients ignore serverSelectionTryOnce; they signal the thread to rescan the topology every half-second until serverSelectionTimeoutMS expires.
MONGOC_URI_SOCKETCHECKINTERVALMS           socketcheckintervalms             Only applies to single threaded clients. If a socket has not been used within this time, its connection is checked with a quick "hello" call before it is used again. Defaults to 5,000ms (5 seconds).
MONGOC_URI_DIRECTCONNECTION                directconnection                  If "true", the driver connects to a single server directly and will not monitor additional servers.  If "false", the driver connects based on the presence and value of the ``replicaSet`` option.
========================================== ================================= =========================================================================================================================================================================================================================

Setting any of the \*TimeoutMS options above to ``0`` will be interpreted as "use the default value".

.. _connection_pool_options:

Connection Pool Options
-----------------------

These options govern the behavior of a :symbol:`mongoc_client_pool_t`. They are ignored by a non-pooled :symbol:`mongoc_client_t`.

========================================== ================================= =========================================================================================================================================================================================================================
Constant                                   Key                               Description
========================================== ================================= =========================================================================================================================================================================================================================
MONGOC_URI_MAXPOOLSIZE                     maxpoolsize                       The maximum number of clients created by a :symbol:`mongoc_client_pool_t` total (both in the pool and checked out). The default value is 100. Once it is reached, :symbol:`mongoc_client_pool_pop` blocks until another thread pushes a client.
MONGOC_URI_MINPOOLSIZE                     minpoolsize                       Deprecated. This option's behavior does not match its name, and its actual behavior will likely hurt performance.
MONGOC_URI_MAXIDLETIMEMS                   maxidletimems                     Not implemented.
MONGOC_URI_WAITQUEUEMULTIPLE               waitqueuemultiple                 Not implemented.
MONGOC_URI_WAITQUEUETIMEOUTMS              waitqueuetimeoutms                The maximum time to wait for a client to become available from the pool.
========================================== ================================= =========================================================================================================================================================================================================================

.. _mongoc_uri_t_write_concern_options:

Write Concern Options
---------------------

========================================== ================================= =======================================================================================================================================================================
Constant                                   Key                               Description
========================================== ================================= =======================================================================================================================================================================
MONGOC_URI_W                               w                                 Determines the write concern (guarantee). Valid values:

                                                                             * 0 = The driver will not acknowledge write operations but will pass or handle any network and socket errors that it receives to the client. If you disable write concern but enable the getLastError command’s w option, w overrides the w option.
                                                                             * 1 = Provides basic acknowledgement of write operations. By specifying 1, you require that a standalone mongod instance, or the primary for replica sets, acknowledge all write operations. For drivers released after the default write concern change, this is the default write concern setting.
                                                                             * majority = For replica sets, if you specify the special majority value to w option, write operations will only return successfully after a majority of the configured replica set members have acknowledged the write operation.
                                                                             * n = For replica sets, if you specify a number n greater than 1, operations with this write concern return only after n members of the set have acknowledged the write. If you set n to a number that is greater than the number of available set members or members that hold data, MongoDB will wait, potentially indefinitely, for these members to become available.
                                                                             * tags = For replica sets, you can specify a tag set to require that all members of the set that have these tags configured return confirmation of the write operation.
MONGOC_URI_WTIMEOUTMS                      wtimeoutms                        The time in milliseconds to wait for replication to succeed, as specified in the w option, before timing out. When wtimeoutMS is 0, write operations will never time out.
MONGOC_URI_JOURNAL                         journal                           Controls whether write operations will wait until the mongod acknowledges the write operations and commits the data to the on disk journal.

                                                                             * true  = Enables journal commit acknowledgement write concern. Equivalent to specifying the getLastError command with the j option enabled.
                                                                             * false = Does not require that mongod commit write operations to the journal before acknowledging the write operation. This is the default option for the journal parameter.
========================================== ================================= =======================================================================================================================================================================

.. _mongoc_uri_t_read_concern_options:

Read Concern Options
--------------------

========================================== ================================= =========================================================================================================================================================================================================================
Constant                                   Key                               Description
========================================== ================================= =========================================================================================================================================================================================================================
MONGOC_URI_READCONCERNLEVEL                readconcernlevel                  The level of isolation for read operations. If the level is left unspecified, the server default will be used. See `readConcern in the MongoDB Manual <https://www.mongodb.com/docs/master/reference/readConcern/>`_ for details.
========================================== ================================= =========================================================================================================================================================================================================================

.. _mongoc_uri_t_read_prefs_options:

Read Preference Options
-----------------------

When connected to a replica set, the driver chooses which member to query using the read preference:

#. Choose members whose type matches "readPreference".
#. From these, if there are any tags sets configured, choose members matching the first tag set. If there are none, fall back to the next tag set and so on, until some members are chosen or the tag sets are exhausted.
#. From the chosen servers, distribute queries randomly among the server with the fastest round-trip times. These include the server with the fastest time and any whose round-trip time is no more than "localThresholdMS" slower.

========================================== ================================= =======================================================================================================================================================================
Constant                                   Key                               Description
========================================== ================================= =======================================================================================================================================================================
MONGOC_URI_READPREFERENCE                  readpreference                    Specifies the replica set read preference for this connection. This setting overrides any secondaryOk value. The read preference values are the following:

                                                                             * primary (default)
                                                                             * primaryPreferred
                                                                             * secondary
                                                                             * secondaryPreferred
                                                                             * nearest
MONGOC_URI_READPREFERENCETAGS              readpreferencetags                A representation of a tag set. See also :ref:`mongoc-read-prefs-tag-sets`.
MONGOC_URI_LOCALTHRESHOLDMS                localthresholdms                  How far to distribute queries, beyond the server with the fastest round-trip time. By default, only servers within 15ms of the fastest round-trip time receive queries.
MONGOC_URI_MAXSTALENESSSECONDS             maxstalenessseconds               The maximum replication lag, in wall clock time, that a secondary can suffer and still be eligible. The smallest allowed value for maxStalenessSeconds is 90 seconds.
========================================== ================================= =======================================================================================================================================================================

.. note::

  When connecting to more than one mongos, libmongoc's localThresholdMS applies only to the selection of mongos servers. The threshold for selecting among replica set members in shards is controlled by the `mongos's localThreshold command line option <https://www.mongodb.com/docs/manual/reference/program/mongos/#cmdoption-localthreshold>`_.

Legacy Options
--------------

For historical reasons, the following options are available. They should however not be used.

========================================== ================================= =======================================================================================================================================================================
Constant                                   Key                               Description
========================================== ================================= =======================================================================================================================================================================
MONGOC_URI_SAFE                            safe                              {true|false} Same as w={1|0}
========================================== ================================= =======================================================================================================================================================================

.. only:: html

  Functions
  ---------

  .. toctree::
    :titlesonly:
    :maxdepth: 1

    mongoc_uri_copy
    mongoc_uri_destroy
    mongoc_uri_get_auth_mechanism
    mongoc_uri_get_auth_source
    mongoc_uri_get_compressors
    mongoc_uri_get_database
    mongoc_uri_get_hosts
    mongoc_uri_get_mechanism_properties
    mongoc_uri_get_option_as_bool
    mongoc_uri_get_option_as_int32
    mongoc_uri_get_option_as_int64
    mongoc_uri_get_option_as_utf8
    mongoc_uri_get_options
    mongoc_uri_get_password
    mongoc_uri_get_read_concern
    mongoc_uri_get_read_prefs
    mongoc_uri_get_read_prefs_t
    mongoc_uri_get_replica_set
    mongoc_uri_get_service
    mongoc_uri_get_ssl
    mongoc_uri_get_string
    mongoc_uri_get_srv_hostname
    mongoc_uri_get_srv_service_name
    mongoc_uri_get_tls
    mongoc_uri_get_username
    mongoc_uri_get_write_concern
    mongoc_uri_has_option
    mongoc_uri_new
    mongoc_uri_new_for_host_port
    mongoc_uri_new_with_error
    mongoc_uri_option_is_bool
    mongoc_uri_option_is_int32
    mongoc_uri_option_is_int64
    mongoc_uri_option_is_utf8
    mongoc_uri_set_auth_mechanism
    mongoc_uri_set_auth_source
    mongoc_uri_set_compressors
    mongoc_uri_set_database
    mongoc_uri_set_mechanism_properties
    mongoc_uri_set_option_as_bool
    mongoc_uri_set_option_as_int32
    mongoc_uri_set_option_as_int64
    mongoc_uri_set_option_as_utf8
    mongoc_uri_set_password
    mongoc_uri_set_read_concern
    mongoc_uri_set_read_prefs_t
    mongoc_uri_set_username
    mongoc_uri_set_write_concern
    mongoc_uri_unescape
