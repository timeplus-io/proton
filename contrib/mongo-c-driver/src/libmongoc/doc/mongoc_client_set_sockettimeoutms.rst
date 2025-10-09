:man_page: mongoc_client_set_sockettimeoutms

mongoc_client_set_sockettimeoutms()
===================================

Synopsis
--------

.. code-block:: c

  void
  mongoc_client_set_sockettimeoutms (mongoc_client_t *client, int32_t timeoutms);

Change the ``sockettimeoutms`` of the :symbol:`mongoc_client_t`.

If ``client`` was obtained from a :symbol:`mongoc_client_pool_t`, the socket timeout is restored to the previous value when calling :symbol:`mongoc_client_pool_push`.

Parameters
----------

* ``client``: A :symbol:`mongoc_client_t`.
* ``timeoutms``: The requested timeout value in milliseconds.

.. seealso::

  | :ref:`URI Connection Options <connection_options>`
