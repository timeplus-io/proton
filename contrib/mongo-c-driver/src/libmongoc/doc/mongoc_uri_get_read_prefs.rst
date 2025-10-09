:man_page: mongoc_uri_get_read_prefs

mongoc_uri_get_read_prefs()
===========================

.. warning::
   .. deprecated:: 1.2.0

      This function is deprecated and should not be used in new code.

      Please use :doc:`mongoc_uri_get_read_prefs_t() <mongoc_uri_get_read_prefs_t>` in new code.

Synopsis
--------

.. code-block:: c

  const bson_t *
  mongoc_uri_get_read_prefs (const mongoc_uri_t *uri);

Parameters
----------

* ``uri``: A :symbol:`mongoc_uri_t`.

Description
-----------

Fetches a bson document containing read preference tag information from a URI. Note that this does not include the read preference mode.

Returns
-------

Returns a bson document that should not be modified or freed if ``uri`` has read preferences, otherwise ``NULL``.

