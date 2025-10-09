/* Copyright 2017 MongoDB Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <stdio.h>
#include <bson/bson.h>

int
main (void)
{
   {
      // bson_append_document_begin example ... begin
      bson_t parent = BSON_INITIALIZER;
      bson_t child;

      bson_append_document_begin (&parent, "foo", 3, &child);
      bson_append_int32 (&child, "baz", 3, 1);
      bson_append_document_end (&parent, &child);

      char *str = bson_as_relaxed_extended_json (&parent, NULL);
      printf ("%s\n", str); // Prints: { "foo" : { "baz" : 1 } }
      bson_free (str);

      bson_destroy (&parent);
      // bson_append_document_begin example ... end
   }

   {
      // bson_array_builder_t example ... begin
      bson_t parent = BSON_INITIALIZER;
      bson_array_builder_t *bab;

      bson_append_array_builder_begin (&parent, "foo", 3, &bab);
      bson_array_builder_append_int32 (bab, 9);
      bson_array_builder_append_int32 (bab, 8);
      bson_array_builder_append_int32 (bab, 7);
      bson_append_array_builder_end (&parent, bab);

      char *str = bson_as_relaxed_extended_json (&parent, NULL);
      printf ("%s\n", str); // Prints: { "foo" : [ 9, 8, 7 ] }
      bson_free (str);

      bson_destroy (&parent);
      // bson_array_builder_t example ... end
   }

   {
      // bson_array_builder_t top-level example ... begin
      bson_t out;
      bson_array_builder_t *bab = bson_array_builder_new ();

      bson_array_builder_append_int32 (bab, 9);
      bson_array_builder_append_int32 (bab, 8);
      bson_array_builder_append_int32 (bab, 7);
      bson_array_builder_build (bab, &out);

      char *str = bson_array_as_relaxed_extended_json (&out, NULL);
      printf ("%s\n", str); // Prints: [ 9, 8, 7 ]
      bson_free (str);

      bson_array_builder_destroy (bab);
      // bson_array_builder_t top-level example ... end
   }

   return 0;
}
