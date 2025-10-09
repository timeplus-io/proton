#ifndef CLIENT_SIDE_ENCRYPTION_HELPERS
#define CLIENT_SIDE_ENCRYPTION_HELPERS

#include <mongoc/mongoc.h>

/* Helper method to find a single document in the given collection and print it
to stdout */
bool
print_one_document (mongoc_collection_t *coll, bson_error_t *error);

/* hex_to_bin parses a hexadecimal string to an array of bytes. `NULL` is
 * returned on error. `len` is set to the number of bytes written. */
uint8_t *
hex_to_bin (const char *hex, uint32_t *len);

#endif
