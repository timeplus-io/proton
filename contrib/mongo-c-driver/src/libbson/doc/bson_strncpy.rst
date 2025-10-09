:man_page: bson_strncpy

bson_strncpy()
==============

Synopsis
--------

.. code-block:: c

  void
  bson_strncpy (char *dst, const char *src, size_t size);

Parameters
----------

* ``dst``: The destination buffer.
* ``src``: The src buffer.
* ``size``: The number of bytes to copy into dst, which must be at least that size.

Description
-----------

Copies up to ``size`` bytes from ``src`` into ``dst``. ``dst`` must be at least ``size`` bytes in size. A trailing ``\0`` is always set.

Does nothing if ``size`` is zero.

``bson_strncpy`` matches the behavior of the C11 standard ``strncpy_s``, rather than ``strncpy``. This means that ``bson_strncpy`` always writes a null terminator to ``dst``, even if ``dst`` is too short to fit the entire string from ``src``. If there is additional space left in ``dst`` after copying ``src``, ``bson_strncpy`` does not fill the remaining space with null characters.
