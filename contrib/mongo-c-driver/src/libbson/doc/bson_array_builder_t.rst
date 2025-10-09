:man_page: bson_array_builder_t

bson_array_builder_t
====================

.. code-block:: c

    typedef struct _bson_array_builder_t bson_array_builder_t;

``bson_array_builder_t`` may be used to build BSON arrays. ``bson_array_builder_t`` internally tracks and uses the array index as a key ("0", "1", "2", ...) when appending elements.

Appending an array value
------------------------

.. code-block:: c

    typedef struct _bson_array_builder_t bson_array_builder_t;

    bool
    bson_append_array_builder_begin (bson_t *bson,
                                     const char *key,
                                     int key_length,
                                     bson_array_builder_t **child);

    bool
    bson_append_array_builder_end (bson_t *bson, bson_array_builder_t *child);

    #define BSON_APPEND_ARRAY_BUILDER_BEGIN(b, key, child) \
      bson_append_array_builder_begin (b, key, (int) strlen (key), child)

``bson_append_array_builder_begin`` may be used to append an array as a value. Example:

.. literalinclude:: ../examples/creating.c
   :language: c
   :start-after: // bson_array_builder_t example ... begin
   :end-before: // bson_array_builder_t example ... end
   :dedent: 6

Creating a top-level array
--------------------------

.. code-block:: c

    bson_array_builder_t * bson_array_builder_new (void);

    bool
    bson_array_builder_build (bson_array_builder_t *bab, bson_t *out);

    BSON_EXPORT (void)
    bson_array_builder_destroy (bson_array_builder_t *bab);

``bson_array_builder_new`` and ``bson_array_builder_build`` may be used to build a top-level BSON array. ``bson_array_builder_build`` initializes and moves BSON data to ``out``. The ``bson_array_builder_t`` may be reused and will start appending a new array at index "0":

Example:

.. literalinclude:: ../examples/creating.c
   :language: c
   :start-after: // bson_array_builder_t top-level example ... begin
   :end-before: // bson_array_builder_t top-level example ... end
   :dedent: 6

Appending values to an array
----------------------------

``bson_array_builder_append_*`` functions are provided to append values to a BSON array. The ``bson_array_builder_append_*`` functions internally use ``bson_append_*`` and provide the array index as a key:

.. code-block:: c

    bool
    bson_array_builder_append_value (bson_array_builder_t *bab,
                                     const bson_value_t *value);


    bool
    bson_array_builder_append_array (bson_array_builder_t *bab,
                                     const bson_t *array);


    bool
    bson_array_builder_append_binary (bson_array_builder_t *bab,
                                      bson_subtype_t subtype,
                                      const uint8_t *binary,
                                      uint32_t length);

    bool
    bson_array_builder_append_bool (bson_array_builder_t *bab, bool value);


    bool
    bson_array_builder_append_code (bson_array_builder_t *bab,
                                    const char *javascript);


    bool
    bson_array_builder_append_code_with_scope (bson_array_builder_t *bab,
                                               const char *javascript,
                                               const bson_t *scope);


    bool
    bson_array_builder_append_dbpointer (bson_array_builder_t *bab,
                                         const char *collection,
                                         const bson_oid_t *oid);


    bool
    bson_array_builder_append_double (bson_array_builder_t *bab, double value);


    bool
    bson_array_builder_append_document (bson_array_builder_t *bab,
                                        const bson_t *value);


    bool
    bson_array_builder_append_document_begin (bson_array_builder_t *bab,
                                              bson_t *child);


    bool
    bson_array_builder_append_document_end (bson_array_builder_t *bab,
                                            bson_t *child);

    bool
    bson_array_builder_append_int32 (bson_array_builder_t *bab, int32_t value);


    bool
    bson_array_builder_append_int64 (bson_array_builder_t *bab, int64_t value);


    bool
    bson_array_builder_append_decimal128 (bson_array_builder_t *bab,
                                          const bson_decimal128_t *value);


    bool
    bson_array_builder_append_iter (bson_array_builder_t *bab,
                                    const bson_iter_t *iter);


    bool
    bson_array_builder_append_minkey (bson_array_builder_t *bab);


    bool
    bson_array_builder_append_maxkey (bson_array_builder_t *bab);


    bool
    bson_array_builder_append_null (bson_array_builder_t *bab);


    bool
    bson_array_builder_append_oid (bson_array_builder_t *bab,
                                   const bson_oid_t *oid);


    bool
    bson_array_builder_append_regex (bson_array_builder_t *bab,
                                     const char *regex,
                                     const char *options);


    bool
    bson_array_builder_append_regex_w_len (bson_array_builder_t *bab,
                                          const char *regex,
                                          int regex_length,
                                          const char *options);

    bool
    bson_array_builder_append_utf8 (bson_array_builder_t *bab,
                                    const char *value,
                                    int length);

    bool
    bson_array_builder_append_symbol (bson_array_builder_t *bab,
                                      const char *value,
                                      int length);

    bool
    bson_array_builder_append_time_t (bson_array_builder_t *bab, time_t value);


    bool
    bson_array_builder_append_timeval (bson_array_builder_t *bab,
                                       struct timeval *value);


    bool
    bson_array_builder_append_date_time (bson_array_builder_t *bab, int64_t value);


    bool
    bson_array_builder_append_now_utc (bson_array_builder_t *bab);


    bool
    bson_array_builder_append_timestamp (bson_array_builder_t *bab,
                                         uint32_t timestamp,
                                         uint32_t increment);

    bool
    bson_array_builder_append_undefined (bson_array_builder_t *bab);

    bool
    bson_array_builder_append_array_builder_begin (bson_array_builder_t *bab,
                                                   bson_array_builder_t **child);

    bool
    bson_array_builder_append_array_builder_end (bson_array_builder_t *bab,
                                                 bson_array_builder_t *child);
