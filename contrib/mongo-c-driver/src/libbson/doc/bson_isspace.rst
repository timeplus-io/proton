:man_page: bson_isspace

bson_isspace()
==============

Synopsis
--------

.. code-block:: c

  bool
  bson_isspace (int c);

Parameters
----------

* ``c``: A character.

Description
-----------

A safer alternative to ``isspace`` with additional bounds checking.

It is equivalent to ``isspace``, excepts always returns false if ``c`` is out of the inclusive bounds [-1, 255].

Returns
-------

A boolean indicating if the ``c`` is considered white-space (as determined by the ``isspace`` function).
