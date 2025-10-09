/*
 * Copyright 2017 MongoDB, Inc.
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

#include "mongoc-config.h"

#ifdef MONGOC_ENABLE_SASL_CYRUS
#include "mongoc-client-private.h"
#include "mongoc-cyrus-private.h"
#include "mongoc-cluster-cyrus-private.h"
#include "mongoc-error.h"
#include "mongoc-trace-private.h"

bool
_mongoc_cluster_auth_node_cyrus (mongoc_cluster_t *cluster,
                                 mongoc_stream_t *stream,
                                 mongoc_server_description_t *sd,
                                 bson_error_t *error)
{
   mongoc_cmd_parts_t parts;
   mongoc_cyrus_t sasl;
   bson_iter_t iter;
   bool ret = false;
   const char *tmpstr;
   /* input into cyrus */
   uint8_t *inbuf = NULL;
   uint32_t inbuf_len = 0;
   /* output from cyrus */
   uint8_t *outbuf = NULL;
   uint32_t outbuf_len = 0;
   mc_shared_tpld td = MC_SHARED_TPLD_NULL;

   bson_t cmd;
   bson_t reply;
   int conv_id = 0;
   mongoc_server_stream_t *server_stream;

   BSON_ASSERT (cluster);
   BSON_ASSERT (stream);

   if (!_mongoc_cyrus_new_from_cluster (&sasl, cluster, stream, sd->host.host, error)) {
      return false;
   }

   for (;;) {
      mongoc_cmd_parts_init (&parts, cluster->client, "$external", MONGOC_QUERY_SECONDARY_OK, &cmd);
      parts.prohibit_lsid = true;

      /* If this is the first step, input buffer is NULL. */
      bson_free (outbuf);
      outbuf = NULL;
      outbuf_len = 0;
      if (!_mongoc_cyrus_step (&sasl, inbuf, inbuf_len, &outbuf, &outbuf_len, error)) {
         goto failure;
      }

      bson_init (&cmd);

      if (sasl.step == 1) {
         _mongoc_cluster_build_sasl_start (&cmd, sasl.credentials.mechanism, (const char *) outbuf, outbuf_len);
      } else {
         _mongoc_cluster_build_sasl_continue (&cmd, conv_id, (const char *) outbuf, outbuf_len);
      }

      TRACE ("SASL: authenticating (step %d)", sasl.step);

      mc_tpld_renew_ref (&td, cluster->client->topology);
      server_stream = _mongoc_cluster_create_server_stream (td.ptr, sd, stream);

      if (!mongoc_cmd_parts_assemble (&parts, server_stream, error)) {
         mongoc_server_stream_cleanup (server_stream);
         bson_destroy (&cmd);
         goto failure;
      }

      if (!mongoc_cluster_run_command_private (cluster, &parts.assembled, &reply, error)) {
         mongoc_server_stream_cleanup (server_stream);
         bson_destroy (&cmd);
         bson_destroy (&reply);
         goto failure;
      }
      mongoc_server_stream_cleanup (server_stream);

      bson_destroy (&cmd);

      if (bson_iter_init_find (&iter, &reply, "done") && bson_iter_as_bool (&iter)) {
         bson_destroy (&reply);
         break;
      }

      conv_id = _mongoc_cluster_get_conversation_id (&reply);

      if (!bson_iter_init_find (&iter, &reply, "payload") || !BSON_ITER_HOLDS_UTF8 (&iter)) {
         MONGOC_DEBUG ("SASL: authentication failed");
         bson_destroy (&reply);
         bson_set_error (error,
                         MONGOC_ERROR_CLIENT,
                         MONGOC_ERROR_CLIENT_AUTHENTICATE,
                         "Received invalid SASL reply from MongoDB server.");
         goto failure;
      }

      tmpstr = bson_iter_utf8 (&iter, &inbuf_len);
      bson_free (inbuf);
      /* include the trailing NULL byte, since base64 decoding expects a NULL
       * terminates string. */
      inbuf = bson_malloc (inbuf_len + 1);
      memcpy (inbuf, tmpstr, inbuf_len + 1);

      bson_destroy (&reply);
      mongoc_cmd_parts_cleanup (&parts);
   }

   TRACE ("%s", "SASL: authenticated");

   ret = true;

failure:
   bson_free (inbuf);
   bson_free (outbuf);
   _mongoc_cyrus_destroy (&sasl);
   mongoc_cmd_parts_cleanup (&parts);
   mc_tpld_drop_ref (&td);

   return ret;
}
#endif
