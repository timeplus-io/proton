/*
 * Copyright 2018-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* -- sphinx-include-start -- */
/* Reports the maximum nested depth of a BSON document. */
#include <bson/bson.h>

#include <assert.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>

typedef struct {
   uint32_t depth;
   uint32_t max_depth;
   bool valid;
} check_depth_t;

bool
_check_depth_document (const bson_iter_t *iter, const char *key, const bson_t *v_document, void *data);

static const bson_visitor_t check_depth_funcs = {
   NULL,
   NULL,
   NULL,
   NULL,
   NULL,
   _check_depth_document,
   _check_depth_document,
   NULL,
};

bool
_check_depth_document (const bson_iter_t *iter, const char *key, const bson_t *v_document, void *data)
{
   check_depth_t *state = (check_depth_t *) data;
   bson_iter_t child;

   BSON_UNUSED (iter);
   BSON_UNUSED (key);

   if (!bson_iter_init (&child, v_document)) {
      fprintf (stderr, "corrupt\n");
      return true; /* cancel */
   }

   state->depth++;
   if (state->depth > state->max_depth) {
      state->valid = false;
      return true; /* cancel */
   }

   bson_iter_visit_all (&child, &check_depth_funcs, state);
   state->depth--;
   return false; /* continue */
}

void
check_depth (const bson_t *bson, uint32_t max_depth)
{
   bson_iter_t iter;
   check_depth_t state = {0};

   if (!bson_iter_init (&iter, bson)) {
      fprintf (stderr, "corrupt\n");
   }

   state.valid = true;
   state.max_depth = max_depth;
   _check_depth_document (&iter, NULL, bson, &state);
   if (!state.valid) {
      printf ("document exceeds maximum depth of %" PRIu32 "\n", state.max_depth);
   } else {
      char *as_json = bson_as_canonical_extended_json (bson, NULL);
      printf ("document %s ", as_json);
      printf ("is valid\n");
      bson_free (as_json);
   }
}

int
main (int argc, char **argv)
{
   bson_reader_t *bson_reader;
   const bson_t *bson;
   bool reached_eof;
   bson_error_t error;

   if (argc != 3) {
      fprintf (stderr, "usage: %s FILE MAX_DEPTH\n", argv[0]);
      fprintf (stderr, "Checks that the depth of the BSON contained in FILE\n");
      fprintf (stderr, "does not exceed MAX_DEPTH\n");
   }

   const char *const filename = argv[1];
   const int max_depth = atoi (argv[2]);

   bson_reader = bson_reader_new_from_file (filename, &error);
   if (!bson_reader) {
      printf ("could not read %s: %s\n", filename, error.message);
      return 1;
   }

   BSON_ASSERT (bson_in_range_signed (uint32_t, max_depth));

   while ((bson = bson_reader_read (bson_reader, &reached_eof))) {
      check_depth (bson, (uint32_t) max_depth);
   }

   if (!reached_eof) {
      printf ("error reading BSON\n");
   }

   bson_reader_destroy (bson_reader);
   return 0;
}
