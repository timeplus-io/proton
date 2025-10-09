#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <bson/bson.h>

int
LLVMFuzzerTestOneInput (const uint8_t *data, size_t size)
{
   char *nt = malloc (size + 1);
   memcpy (nt, data, size);
   nt[size] = '\0';
   bson_error_t error;

   bson_t b;
   if (bson_init_from_json (&b, nt, -1, &error)) {
      bson_destroy (&b);
   }

   free (nt);
   return 0;
}
