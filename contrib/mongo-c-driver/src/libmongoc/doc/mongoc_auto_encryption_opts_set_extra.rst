:man_page: mongoc_auto_encryption_opts_set_extra

mongoc_auto_encryption_opts_set_extra()
=======================================

Synopsis
--------

.. code-block:: c

   void
   mongoc_auto_encryption_opts_set_extra (mongoc_auto_encryption_opts_t *opts,
                                          const bson_t *extra);


Parameters
----------

* ``opts``: The :symbol:`mongoc_auto_encryption_opts_t`
* ``extra``: A :symbol:`bson_t` of additional options.

``extra`` is a :symbol:`bson_t` containing any of the following optional fields:

* ``mongocryptdURI`` set to a URI to connect to the mongocryptd process (default is "mongodb://localhost:27020").
* ``mongocryptdBypassSpawn`` set to true to prevent the driver from spawning the mongocryptd process (default behavior is to spawn).
* ``mongocryptdSpawnPath`` set to a path (with trailing slash) to search for mongocryptd (defaults to empty string and uses default system paths).
* ``mongocryptdSpawnArgs`` set to an array of string arguments to pass to ``mongocryptd`` when spawning (defaults to ``[ "--idleShutdownTimeoutSecs=60" ]``).
* ``cryptSharedLibPath`` - Set a filepath string referring to a ``crypt_shared``
  library file. Unset by default.

  * If not set (the default), ``libmongocrypt`` will attempt to load
    ``crypt_shared`` using the host system's default dynamic-library-search
    system.
  * If set, the given path should identify the ``crypt_shared`` dynamic library
    file itself, not the directory that contains it.
  * If the given path is a relative path and the first path component is
    ``$ORIGIN``, the ``$ORIGIN`` component will be replaced with the absolute
    path to the directory containing the ``libmongocrypt`` library in use by the
    application.

    .. note:: No other ``RPATH``/``RUNPATH``-style substitutions are available.

  * If the given path is a relative path, the path will be resolved relative to
    the working directory of the operating system process.
  * If this option is set and ``libmongocrypt`` fails to load ``crypt_shared`` from the
    given filepath, ``libmongocrypt`` will fail to initialize and will not
    attempt to search for ``crypt_shared`` in any other locations.

* ``cryptSharedLibRequired`` - If set to ``true``, and ``libmongocrypt`` fails
  to load a ``crypt_shared`` library, initialization of auto-encryption will
  fail immediately and will not attempt to spawn ``mongocryptd``.

  If set to ``false`` (the default), ``cryptSharedLibPath`` is not set, *and*
  ``libmongocrypt`` fails to load ``crypt_shared``, then ``libmongocrypt`` will
  proceed without ``crypt_shared`` and fall back to using ``mongocryptd``.

.. seealso::

  | :symbol:`mongoc_client_enable_auto_encryption()`

  | `In-Use Encryption <in-use-encryption_>`_

  | MongoDB Manual documentation for ``crypt_shared``: `Automatic Encryption Shared Library <https://www.mongodb.com/docs/manual/core/queryable-encryption/reference/shared-library/>`_

  | MongoDB Manual documentation for ``mongocryptd``: `Install and Configure mongocryptd <https://www.mongodb.com/docs/manual/core/queryable-encryption/reference/mongocryptd/>`_

