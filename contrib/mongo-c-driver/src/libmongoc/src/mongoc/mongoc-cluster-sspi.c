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

#ifdef MONGOC_ENABLE_SASL_SSPI
#include "mongoc-client-private.h"
#include "mongoc-cluster-sspi-private.h"
#include "mongoc-cluster-sasl-private.h"
#include "mongoc-sasl-private.h"
#include "mongoc-sspi-private.h"
#include "mongoc-error.h"
#include "mongoc-util-private.h"


static mongoc_sspi_client_state_t *
_mongoc_cluster_sspi_new (mongoc_uri_t *uri, mongoc_stream_t *stream, const char *hostname)
{
   WCHAR *service; /* L"serviceName@hostname@REALM" */
   const char *service_name = "mongodb";
   ULONG flags = ISC_REQ_MUTUAL_AUTH;
   const char *service_realm = NULL;
   char *service_ascii = NULL;
   mongoc_sspi_client_state_t *state;
   size_t service_ascii_len;
   size_t tmp_creds_len;
   bson_t properties;
   bson_iter_t iter;
   char real_name[BSON_HOST_NAME_MAX + 1];
   int service_len;
   WCHAR *pass = NULL;
   WCHAR *user = NULL;
   size_t user_len = 0;
   size_t pass_len = 0;
   int res;

   state = (mongoc_sspi_client_state_t *) bson_malloc0 (sizeof *state);
   _mongoc_sasl_set_properties (&state->sasl, uri);

   if (state->sasl.canonicalize_host_name &&
       _mongoc_sasl_get_canonicalized_name (stream, real_name, sizeof real_name)) {
      hostname = real_name;
   }

   /* service realm is an SSPI-specific feature */
   if (mongoc_uri_get_mechanism_properties (uri, &properties) &&
       bson_iter_init_find_case (&iter, &properties, "SERVICE_REALM") && BSON_ITER_HOLDS_UTF8 (&iter)) {
      service_realm = bson_iter_utf8 (&iter, NULL);
      service_ascii = bson_strdup_printf ("%s@%s@%s", service_name, hostname, service_realm);
   } else {
      service_ascii = bson_strdup_printf ("%s@%s", service_name, hostname);
   }
   service_ascii_len = strlen (service_ascii);

   /* this is donated to the sspi */
   service = bson_malloc0 ((service_ascii_len + 1) * sizeof (WCHAR));
   service_len =
      MultiByteToWideChar (CP_UTF8, 0, service_ascii, (int) service_ascii_len, service, (int) service_ascii_len);
   service[service_len] = L'\0';
   bson_free (service_ascii);

   if (state->sasl.pass) {
      tmp_creds_len = strlen (state->sasl.pass);

      /* this is donated to the sspi */
      pass = bson_malloc0 ((tmp_creds_len + 1) * sizeof (WCHAR));
      pass_len = MultiByteToWideChar (CP_UTF8, 0, state->sasl.pass, (int) tmp_creds_len, pass, (int) tmp_creds_len);
      pass[pass_len] = L'\0';
   }

   if (state->sasl.user) {
      tmp_creds_len = strlen (state->sasl.user);

      /* this is donated to the sspi */
      user = bson_malloc0 ((tmp_creds_len + 1) * sizeof (WCHAR));
      user_len = MultiByteToWideChar (CP_UTF8, 0, state->sasl.user, (int) tmp_creds_len, user, (int) tmp_creds_len);
      user[user_len] = L'\0';
   }

   res = _mongoc_sspi_auth_sspi_client_init (
      service, flags, user, (ULONG) user_len, NULL, 0, pass, (ULONG) pass_len, state);

   if (res != MONGOC_SSPI_AUTH_GSS_ERROR) {
      return state;
   }
   bson_free (state);
   return NULL;
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_cluster_auth_node_sspi --
 *
 *       Perform authentication for a cluster node using SSPI
 *
 * Returns:
 *       true if successful; otherwise false and @error is set.
 *
 * Side effects:
 *       error may be set.
 *
 *--------------------------------------------------------------------------
 */

bool
_mongoc_cluster_auth_node_sspi (mongoc_cluster_t *cluster,
                                mongoc_stream_t *stream,
                                mongoc_server_description_t *sd,
                                bson_error_t *error)
{
   mongoc_cmd_parts_t parts;
   mongoc_sspi_client_state_t *state;
   SEC_CHAR *buf = NULL;
   bson_iter_t iter;
   uint32_t buflen;
   bson_t reply;
   const char *tmpstr;
   int conv_id;
   bson_t cmd;
   int res = MONGOC_SSPI_AUTH_GSS_CONTINUE;
   int step;
   mongoc_server_stream_t *server_stream;
   bool ret = false;
   mc_shared_tpld td = MC_SHARED_TPLD_NULL;

   state = _mongoc_cluster_sspi_new (cluster->uri, stream, sd->host.host);

   if (!state) {
      bson_set_error (
         error, MONGOC_ERROR_CLIENT, MONGOC_ERROR_CLIENT_AUTHENTICATE, "Couldn't initialize SSPI service.");
      goto failure;
   }

   for (step = 0;; step++) {
      mongoc_cmd_parts_init (&parts, cluster->client, "$external", MONGOC_QUERY_SECONDARY_OK, &cmd);
      parts.prohibit_lsid = true;
      bson_init (&cmd);

      if (res == MONGOC_SSPI_AUTH_GSS_CONTINUE) {
         res = _mongoc_sspi_auth_sspi_client_step (state, buf);
      } else if (res == MONGOC_SSPI_AUTH_GSS_COMPLETE) {
         char *response;
         size_t tmp_creds_len = strlen (state->sasl.user);

         res = _mongoc_sspi_auth_sspi_client_unwrap (state, buf);
         response = bson_strdup (state->response);
         _mongoc_sspi_auth_sspi_client_wrap (state, response, (SEC_CHAR *) state->sasl.user, (ULONG) tmp_creds_len, 0);
         bson_free (response);
      }

      if (res == MONGOC_SSPI_AUTH_GSS_ERROR) {
         bson_set_error (error, MONGOC_ERROR_CLIENT, MONGOC_ERROR_CLIENT_AUTHENTICATE, "Received invalid SSPI data.");

         mongoc_cmd_parts_cleanup (&parts);
         bson_destroy (&cmd);
         goto failure;
      }

      if (step == 0) {
         _mongoc_cluster_build_sasl_start (&cmd, "GSSAPI", state->response, (uint32_t) strlen (state->response));
      } else {
         if (state->response) {
            _mongoc_cluster_build_sasl_continue (&cmd, conv_id, state->response, (uint32_t) strlen (state->response));
         } else {
            _mongoc_cluster_build_sasl_continue (&cmd, conv_id, "", 0);
         }
      }

      mc_tpld_renew_ref (&td, cluster->client->topology);
      server_stream = _mongoc_cluster_create_server_stream (td.ptr, sd, stream);

      if (!mongoc_cmd_parts_assemble (&parts, server_stream, error)) {
         mongoc_server_stream_cleanup (server_stream);
         mongoc_cmd_parts_cleanup (&parts);
         bson_destroy (&cmd);
         goto failure;
      }

      if (!mongoc_cluster_run_command_private (cluster, &parts.assembled, &reply, error)) {
         mongoc_server_stream_cleanup (server_stream);
         mongoc_cmd_parts_cleanup (&parts);
         bson_destroy (&cmd);
         bson_destroy (&reply);
         goto failure;
      }

      mongoc_server_stream_cleanup (server_stream);
      mongoc_cmd_parts_cleanup (&parts);
      bson_destroy (&cmd);

      if (bson_iter_init_find (&iter, &reply, "done") && bson_iter_as_bool (&iter)) {
         bson_destroy (&reply);
         break;
      }

      conv_id = _mongoc_cluster_get_conversation_id (&reply);

      if (!bson_iter_init_find (&iter, &reply, "payload") || !BSON_ITER_HOLDS_UTF8 (&iter)) {
         bson_destroy (&reply);
         bson_set_error (error,
                         MONGOC_ERROR_CLIENT,
                         MONGOC_ERROR_CLIENT_AUTHENTICATE,
                         "Received invalid SASL reply from MongoDB server.");
         goto failure;
      }


      tmpstr = bson_iter_utf8 (&iter, &buflen);
      bson_free (buf);
      buf = bson_malloc (sizeof (SEC_CHAR) * (buflen + 1));
      memcpy (buf, tmpstr, buflen);
      buf[buflen] = (SEC_CHAR) 0;

      bson_destroy (&reply);
   }

   ret = true;
failure:
   mc_tpld_drop_ref (&td);
   bson_free (buf);
   bson_free (state);
   return ret;
}

#endif
