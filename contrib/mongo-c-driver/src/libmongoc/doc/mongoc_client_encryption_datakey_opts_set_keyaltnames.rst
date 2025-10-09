:man_page: mongoc_client_encryption_datakey_opts_set_keyaltnames

mongoc_client_encryption_datakey_opts_set_keyaltnames()
=======================================================

Synopsis
--------

.. code-block:: c

   void
   mongoc_client_encryption_datakey_opts_set_keyaltnames (
      mongoc_client_encryption_datakey_opts_t *opts,
      char **keyaltnames,
      uint32_t keyaltnames_count);

Sets a list of alternate name strings that can be used to identify a data key. Key alternate names must be unique.

Parameters
----------

* ``opts``: A :symbol:`mongoc_client_encryption_datakey_opts_t`
* ``keyaltnames``: An array of strings.
* ``keyaltnames_count``: The number of strings in ``keyaltnames``.

.. seealso::

  | :symbol:`mongoc_client_encryption_encrypt_opts_set_keyaltname`

