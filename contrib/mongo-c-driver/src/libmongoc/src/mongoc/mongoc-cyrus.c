/*
 * Copyright 2013 MongoDB, Inc.
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

#include <string.h>

#include "mongoc-error.h"
#include "mongoc-cyrus-private.h"
#include "mongoc-util-private.h"
#include "mongoc-trace-private.h"
#include "common-b64-private.h"

#undef MONGOC_LOG_DOMAIN
#define MONGOC_LOG_DOMAIN "CYRUS-SASL"

bool
_mongoc_cyrus_set_mechanism (mongoc_cyrus_t *sasl, const char *mechanism, bson_error_t *error)
{
   bson_string_t *str = bson_string_new ("");
   const char **mechs = sasl_global_listmech ();
   int i = 0;
   bool ok = false;

   BSON_ASSERT (sasl);

   for (i = 0; mechs[i]; i++) {
      if (!strcmp (mechs[i], mechanism)) {
         ok = true;
         break;
      }
      bson_string_append (str, mechs[i]);
      if (mechs[i + 1]) {
         bson_string_append (str, ",");
      }
   }

   if (ok) {
      bson_free (sasl->credentials.mechanism);
      sasl->credentials.mechanism = mechanism ? bson_strdup (mechanism) : NULL;
   } else {
      bson_set_error (error,
                      MONGOC_ERROR_SASL,
                      SASL_NOMECH,
                      "SASL Failure: Unsupported mechanism by client: %s. "
                      "Available mechanisms: %s",
                      mechanism,
                      str->str);
   }

   bson_string_free (str, true);
   return ok;
}


static int
_mongoc_cyrus_get_pass (mongoc_cyrus_t *sasl, int param_id, const char **result, unsigned *result_len)
{
   BSON_ASSERT (sasl);
   BSON_ASSERT (param_id == SASL_CB_PASS);

   if (result) {
      *result = sasl->credentials.pass;
   }

   if (result_len) {
      *result_len = sasl->credentials.pass ? (unsigned) strlen (sasl->credentials.pass) : 0;
   }

   return (sasl->credentials.pass != NULL) ? SASL_OK : SASL_FAIL;
}


static int
_mongoc_cyrus_canon_user (sasl_conn_t *conn,
                          mongoc_cyrus_t *sasl,
                          const char *in,
                          unsigned inlen,
                          unsigned flags,
                          const char *user_realm,
                          char *out,
                          unsigned out_max,
                          unsigned *out_len)
{
   BSON_UNUSED (conn);
   BSON_UNUSED (sasl);
   BSON_UNUSED (flags);
   BSON_UNUSED (user_realm);
   BSON_UNUSED (out_max);

   TRACE ("Canonicalizing %s (%" PRIu32 ")\n", in, inlen);
   strcpy (out, in);
   *out_len = inlen;
   return SASL_OK;
}

static int
_mongoc_cyrus_get_user (mongoc_cyrus_t *sasl, int param_id, const char **result, unsigned *result_len)
{
   BSON_ASSERT (sasl);
   BSON_ASSERT ((param_id == SASL_CB_USER) || (param_id == SASL_CB_AUTHNAME));

   if (result) {
      *result = sasl->credentials.user;
   }

   if (result_len) {
      *result_len = sasl->credentials.user ? (unsigned) strlen (sasl->credentials.user) : 0;
   }

   return (sasl->credentials.user != NULL) ? SASL_OK : SASL_FAIL;
}

static const char *
sasl_verify_type_to_str (sasl_verify_type_t type)
{
   switch (type) {
   case SASL_VRFY_PLUGIN:
      return "SASL_VRFY_PLUGIN";
   case SASL_VRFY_CONF:
      return "SASL_VRFY_CONF";
   case SASL_VRFY_PASSWD:
      return "SASL_VRFY_PASSWD";
   case SASL_VRFY_OTHER:
      return "SASL_VRFY_OTHER";
   default:
      return "Unknown";
   }
}

int
_mongoc_cyrus_verifyfile_cb (void *context, const char *file, sasl_verify_type_t type)
{
   TRACE ("Attempting to load file: `%s`. Type is %s\n", file, sasl_verify_type_to_str (type));

#ifdef _WIN32
   // On Windows, Cyrus SASL hard-codes the plugin path.
   // Only permit loading plugin from user configured path to prevent unintentional library loading.
   if (type == SASL_VRFY_PLUGIN) {
      const char *path_prefix = MONGOC_CYRUS_PLUGIN_PATH_PREFIX;
      bool has_valid_prefix = (path_prefix && file == strstr (file, path_prefix));
      // Check if `file` has necessary prefix.
      if (has_valid_prefix) {
         return SASL_OK;
      }
      MONGOC_WARNING ("Refusing to load Cyrus SASL plugin at: '%s'. If needed, set CYRUS_PLUGIN_PATH_PREFIX (currently "
                      "'%s') to the absolute path prefix of the plugin during build configuration of the C Driver.",
                      file,
                      path_prefix ? path_prefix : "(unset)");
      return SASL_CONTINUE;
   }
#endif

   return SASL_OK;
}


void
_mongoc_cyrus_init (mongoc_cyrus_t *sasl)
{
   sasl_callback_t callbacks[] = {{SASL_CB_AUTHNAME, SASL_CALLBACK_FN (_mongoc_cyrus_get_user), sasl},
                                  {SASL_CB_USER, SASL_CALLBACK_FN (_mongoc_cyrus_get_user), sasl},
                                  {SASL_CB_PASS, SASL_CALLBACK_FN (_mongoc_cyrus_get_pass), sasl},
                                  {SASL_CB_CANON_USER, SASL_CALLBACK_FN (_mongoc_cyrus_canon_user), sasl},
                                  {SASL_CB_VERIFYFILE, SASL_CALLBACK_FN (_mongoc_cyrus_verifyfile_cb), NULL},
                                  {SASL_CB_LIST_END}};

   BSON_ASSERT (sasl);

   memset (sasl, 0, sizeof *sasl);

   memcpy (&sasl->callbacks, callbacks, sizeof callbacks);

   sasl->done = false;
   sasl->step = 0;
   sasl->conn = NULL;
   sasl->interact = NULL;
   sasl->credentials.mechanism = NULL;
   sasl->credentials.user = NULL;
   sasl->credentials.pass = NULL;
   sasl->credentials.service_name = NULL;
   sasl->credentials.service_host = NULL;
}

bool
_mongoc_cyrus_new_from_cluster (
   mongoc_cyrus_t *sasl, mongoc_cluster_t *cluster, mongoc_stream_t *stream, const char *hostname, bson_error_t *error)
{
   const char *mechanism;
   char real_name[BSON_HOST_NAME_MAX + 1];

   _mongoc_cyrus_init (sasl);

   mechanism = mongoc_uri_get_auth_mechanism (cluster->uri);
   if (!mechanism) {
      mechanism = "GSSAPI";
   }

   if (!_mongoc_cyrus_set_mechanism (sasl, mechanism, error)) {
      _mongoc_cyrus_destroy (sasl);
      return false;
   }

   _mongoc_sasl_set_pass ((mongoc_sasl_t *) sasl, mongoc_uri_get_password (cluster->uri));
   _mongoc_sasl_set_user ((mongoc_sasl_t *) sasl, mongoc_uri_get_username (cluster->uri));
   _mongoc_sasl_set_properties ((mongoc_sasl_t *) sasl, cluster->uri);

   /*
    * If the URI requested canonicalizeHostname, we need to resolve the real
    * hostname for the IP Address and pass that to the SASL layer. Some
    * underlying GSSAPI layers will do this for us, but can be disabled in
    * their config (krb.conf).
    *
    * This allows the consumer to specify canonicalizeHostname=true in the URI
    * and have us do that for them.
    *
    * See CDRIVER-323 for more information.
    */
   if (sasl->credentials.canonicalize_host_name &&
       _mongoc_sasl_get_canonicalized_name (stream, real_name, sizeof real_name)) {
      _mongoc_sasl_set_service_host ((mongoc_sasl_t *) sasl, real_name);
   } else {
      _mongoc_sasl_set_service_host ((mongoc_sasl_t *) sasl, hostname);
   }
   return true;
}


void
_mongoc_cyrus_destroy (mongoc_cyrus_t *sasl)
{
   BSON_ASSERT (sasl);

   if (sasl->conn) {
      sasl_dispose (&sasl->conn);
   }

   bson_free (sasl->credentials.user);
   bson_free (sasl->credentials.pass);
   bson_free (sasl->credentials.mechanism);
   bson_free (sasl->credentials.service_name);
   bson_free (sasl->credentials.service_host);
}


static bool
_mongoc_cyrus_is_failure (int status, bson_error_t *error)
{
   bool ret = (status < 0);

   TRACE ("Got status: %d ok is %d, continue=%d interact=%d\n", status, SASL_OK, SASL_CONTINUE, SASL_INTERACT);
   if (ret) {
      switch (status) {
      case SASL_NOMEM:
         bson_set_error (error, MONGOC_ERROR_SASL, status, "SASL Failure: insufficient memory.");
         break;
      case SASL_NOMECH: {
         bson_string_t *str = bson_string_new ("available mechanisms: ");
         const char **mechs = sasl_global_listmech ();
         int i = 0;

         for (i = 0; mechs[i]; i++) {
            bson_string_append (str, mechs[i]);
            if (mechs[i + 1]) {
               bson_string_append (str, ",");
            }
         }
         bson_set_error (
            error, MONGOC_ERROR_SASL, status, "SASL Failure: failure to negotiate mechanism (%s)", str->str);
         bson_string_free (str, 0);
      } break;
      case SASL_BADPARAM:
         bson_set_error (error,
                         MONGOC_ERROR_SASL,
                         status,
                         "Bad parameter supplied. Please file a bug "
                         "with mongo-c-driver.");
         break;
      default:
         bson_set_error (
            error, MONGOC_ERROR_SASL, status, "SASL Failure: (%d): %s", status, sasl_errstring (status, NULL, NULL));
         break;
      }
   }

   return ret;
}


static bool
_mongoc_cyrus_start (mongoc_cyrus_t *sasl, uint8_t **outbuf, uint32_t *outbuflen, bson_error_t *error)
{
   const char *service_name = "mongodb";
   const char *service_host = "";
   const char *mechanism = NULL;
   const char *raw = NULL;
   unsigned raw_len = 0;
   int status;

   BSON_ASSERT (sasl);
   BSON_ASSERT (outbuf);
   BSON_ASSERT (outbuflen);

   if (sasl->credentials.service_name) {
      service_name = sasl->credentials.service_name;
   }

   if (sasl->credentials.service_host) {
      service_host = sasl->credentials.service_host;
   }

   status = sasl_client_new (service_name, service_host, NULL, NULL, sasl->callbacks, 0, &sasl->conn);
   TRACE ("Created new sasl client %s", status == SASL_OK ? "successfully" : "UNSUCCESSFULLY");
   if (_mongoc_cyrus_is_failure (status, error)) {
      return false;
   }

   status = sasl_client_start (sasl->conn, sasl->credentials.mechanism, &sasl->interact, &raw, &raw_len, &mechanism);
   TRACE ("Started the sasl client %s", status == SASL_CONTINUE ? "successfully" : "UNSUCCESSFULLY");
   if (_mongoc_cyrus_is_failure (status, error)) {
      return false;
   }

   if ((0 != strcasecmp (mechanism, "GSSAPI")) && (0 != strcasecmp (mechanism, "PLAIN"))) {
      bson_set_error (error, MONGOC_ERROR_SASL, SASL_NOMECH, "SASL Failure: invalid mechanism \"%s\"", mechanism);
      return false;
   }

   *outbuflen = 0;
   const size_t outbuf_capacity = mcommon_b64_ntop_calculate_target_size (raw_len);
   *outbuf = bson_malloc (outbuf_capacity);

   const int b64_ret = mcommon_b64_ntop ((uint8_t *) raw, raw_len, (char *) *outbuf, outbuf_capacity);
   if (b64_ret < 0) {
      bson_set_error (
         error, MONGOC_ERROR_SASL, MONGOC_ERROR_CLIENT_AUTHENTICATE, "Unable to base64 encode client SASL message");
      return false;
   } else {
      BSON_ASSERT (bson_in_range_signed (uint32_t, b64_ret));
      *outbuflen = (uint32_t) b64_ret;
   }

   return true;
}


bool
_mongoc_cyrus_step (mongoc_cyrus_t *sasl,
                    const uint8_t *inbuf,
                    uint32_t inbuflen,
                    uint8_t **outbuf,
                    uint32_t *outbuflen,
                    bson_error_t *error)
{
   const char *raw = NULL;
   unsigned rawlen = 0;
   int status;

   BSON_ASSERT (sasl);
   if (sasl->step > 1) {
      BSON_ASSERT (inbuf);
   }
   BSON_ASSERT (outbuf);
   BSON_ASSERT (outbuflen);

   TRACE ("Running %d, inbuflen: %" PRIu32, sasl->step, inbuflen);
   sasl->step++;

   if (sasl->step == 1) {
      return _mongoc_cyrus_start (sasl, outbuf, outbuflen, error);
   } else if (sasl->step >= 10) {
      bson_set_error (error, MONGOC_ERROR_SASL, SASL_NOTDONE, "SASL Failure: maximum steps detected");
      return false;
   }

   TRACE ("Running %d, inbuflen: %" PRIu32, sasl->step, inbuflen);
   if (!inbuflen) {
      bson_set_error (error,
                      MONGOC_ERROR_SASL,
                      MONGOC_ERROR_CLIENT_AUTHENTICATE,
                      "SASL Failure: no payload provided from server: %s",
                      sasl_errdetail (sasl->conn));
      return false;
   }

   unsigned int decoded_len = 0;
   const size_t decoded_capacity = mcommon_b64_pton_calculate_target_size (inbuflen);

   char *const decoded = bson_malloc (decoded_capacity);
   {
      const int b64_ret = mcommon_b64_pton ((char *) inbuf, (uint8_t *) decoded, decoded_capacity);
      if (b64_ret < 0) {
         bson_set_error (
            error, MONGOC_ERROR_SASL, MONGOC_ERROR_CLIENT_AUTHENTICATE, "Unable to base64 decode client SASL message");
         bson_free (decoded);
         bson_free (*outbuf);
         *outbuf = NULL;
         return false;
      } else {
         /* Set the output length to the number of bytes actually decoded to
          * excluding the NULL. */
         decoded_len = (unsigned int) b64_ret;
      }
   }

   TRACE ("%s", "Running client_step");
   status = sasl_client_step (sasl->conn, decoded, decoded_len, &sasl->interact, &raw, &rawlen);
   TRACE ("%s sent a client step", status == SASL_OK ? "Successfully" : "UNSUCCESSFULLY");
   if (_mongoc_cyrus_is_failure (status, error)) {
      bson_free (decoded);
      return false;
   }

   *outbuflen = 0;
   const size_t outbuf_capacity = mcommon_b64_ntop_calculate_target_size (rawlen);
   *outbuf = bson_malloc0 (outbuf_capacity);
   {
      const int b64_ret = mcommon_b64_ntop ((const uint8_t *) raw, rawlen, (char *) *outbuf, outbuf_capacity);
      if (b64_ret < 0) {
         bson_set_error (
            error, MONGOC_ERROR_SASL, MONGOC_ERROR_CLIENT_AUTHENTICATE, "Unable to base64 encode client SASL message");
         bson_free (decoded);
         bson_free (*outbuf);
         *outbuf = NULL;
         return false;
      } else {
         /* Set the output length to the number of characters written excluding
          * the NULL. */
         BSON_ASSERT (bson_in_range_signed (uint32_t, b64_ret));
         *outbuflen = (uint32_t) b64_ret;
      }
   }

   bson_free (decoded);
   return true;
}

#endif
