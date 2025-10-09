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


#ifndef TEST_LIBMONGOC_H
#define TEST_LIBMONGOC_H


struct _TestSuite;
struct _bson_t;
struct _server_version_t;


void
test_libmongoc_init (struct _TestSuite *suite, const char *name, int argc, char **argv);
void
test_libmongoc_destroy (struct _TestSuite *suite);


mongoc_database_t *
get_test_database (mongoc_client_t *client);
char *
gen_collection_name (const char *prefix);
mongoc_collection_t *
get_test_collection (mongoc_client_t *client, const char *prefix);
void
capture_logs (bool capture);
void
clear_captured_logs (void);
bool
has_captured_log (mongoc_log_level_t level, const char *msg);
void
assert_all_captured_logs_have_prefix (const char *prefix);
bool
has_captured_logs (void);
void
print_captured_logs (const char *prefix);
int64_t
get_future_timeout_ms (void);
char *
test_framework_getenv (const char *name);
char *
test_framework_getenv_required (const char *name);
bool
test_framework_getenv_bool (const char *name);
int64_t
test_framework_getenv_int64 (const char *name, int64_t default_value);
char *
test_framework_get_host (void);
uint16_t
test_framework_get_port (void);
char *
test_framework_get_host_and_port (void);
char *
test_framework_get_admin_user (void);
char *
test_framework_get_admin_password (void);
bool
test_framework_get_ssl (void);
char *
test_framework_add_user_password (const char *uri_str, const char *user, const char *password);
char *
test_framework_add_user_password_from_env (const char *uri_str);
char *
test_framework_get_uri_str_no_auth (const char *database_name);
char *
test_framework_get_uri_str (void);
char *
test_framework_get_unix_domain_socket_uri_str (void);
char *
test_framework_get_unix_domain_socket_path_escaped (void);
mongoc_uri_t *
test_framework_get_uri (void);
mongoc_uri_t *
test_framework_get_uri_multi_mongos_loadbalanced (void);
bool
test_framework_uri_apply_multi_mongos (mongoc_uri_t *uri, bool use_multi, bson_error_t *error);
size_t
test_framework_mongos_count (void);
char *
test_framework_replset_name (void);
size_t
test_framework_replset_member_count (void);
size_t
test_framework_data_nodes_count (void);
size_t
test_framework_server_count (void);

#ifdef MONGOC_ENABLE_SSL
const mongoc_ssl_opt_t *
test_framework_get_ssl_opts (void);
#endif
void
test_framework_set_ssl_opts (mongoc_client_t *client);
void
test_framework_set_pool_ssl_opts (mongoc_client_pool_t *pool);

mongoc_server_api_t *
test_framework_get_default_server_api (void);

mongoc_client_t *
test_framework_new_default_client (void);
mongoc_client_t *
test_framework_client_new_no_server_api (void);
mongoc_client_t *
test_framework_client_new (const char *uri_str, const mongoc_server_api_t *api);
mongoc_client_t *
test_framework_client_new_from_uri (const mongoc_uri_t *uri, const mongoc_server_api_t *api);

mongoc_client_pool_t *
test_framework_new_default_client_pool (void);
mongoc_client_pool_t *
test_framework_client_pool_new_from_uri (const mongoc_uri_t *uri, const mongoc_server_api_t *api);

bool
test_framework_is_mongos (void);
bool
test_framework_is_replset (void);
// `test_framework_is_mongohouse` returns true if configured to test
// mongohoused (used for Atlas Data Lake).
// See: "Atlas Data Lake Tests" in the MongoDB Specifications.
bool
test_framework_is_mongohouse (void);
bool
test_framework_server_is_secondary (mongoc_client_t *client, uint32_t server_id);
int64_t
test_framework_session_timeout_minutes (void);
void
test_framework_get_max_wire_version (int64_t *max_version);
bool
test_framework_clustertime_supported (void);
bool
test_framework_max_wire_version_at_least (int version);
int64_t
test_framework_max_write_batch_size (void);

bool
test_framework_has_auth (void);
int
test_framework_skip_if_auth (void);
int
test_framework_skip_if_no_auth (void);
int
test_framework_skip_if_no_sessions (void);
int
test_framework_skip_if_no_cluster_time (void);
int
test_framework_skip_if_crypto (void);
int
test_framework_skip_if_no_crypto (void);
int
test_framework_skip_if_no_mongohouse (void);
int
test_framework_skip_if_mongos (void);
int
test_framework_skip_if_replset (void);
int
test_framework_skip_if_single (void);
int
test_framework_skip_if_windows (void);
int
test_framework_skip_if_no_uds (void); /* skip if no Unix domain socket */
int
test_framework_skip_if_no_txns (void);
int
test_framework_skip_if_not_mongos (void);
int
test_framework_skip_if_not_replset (void);
int
test_framework_skip_if_not_single (void);
int
test_framework_skip_if_offline (void);
int
test_framework_skip_if_slow (void);
int
test_framework_skip_if_slow_or_live (void);

#define WIRE_VERSION_CHECK_DECLS(wv)                                  \
   int test_framework_skip_if_max_wire_version_less_than_##wv (void); \
   int test_framework_skip_if_max_wire_version_more_than_##wv (void); \
   int test_framework_skip_if_rs_version_##wv (void);                 \
   int test_framework_skip_if_not_rs_version_##wv (void);

WIRE_VERSION_CHECK_DECLS (7)
WIRE_VERSION_CHECK_DECLS (8)
WIRE_VERSION_CHECK_DECLS (9)
/* wire versions 10, 11, 12 were internal to the 5.0 release cycle */
WIRE_VERSION_CHECK_DECLS (13)
/* wire version 14 begins with the 5.1 prerelease. */
WIRE_VERSION_CHECK_DECLS (14)
/* wire version 17 begins with the 6.0 release. */
WIRE_VERSION_CHECK_DECLS (17)
/* wire version 19 begins with the 6.2 release. */
WIRE_VERSION_CHECK_DECLS (19)
/* wire version 21 begins with the 7.0 release. */
WIRE_VERSION_CHECK_DECLS (21)
/* wire version 22 begins with the 7.1 release. */
WIRE_VERSION_CHECK_DECLS (22)
/* wire version 23 begins with the 7.2 release. */
WIRE_VERSION_CHECK_DECLS (23)
/* wire version 24 begins with the 7.3 release. */
WIRE_VERSION_CHECK_DECLS (24)
/* wire version 25 begins with the 8.0 release. */
WIRE_VERSION_CHECK_DECLS (25)

#undef WIRE_VERSION_CHECK_DECLS

typedef struct _debug_stream_stats_t {
   mongoc_client_t *client;
   int n_destroyed;
   int n_failed;
} debug_stream_stats_t;

void
test_framework_set_debug_stream (mongoc_client_t *client, debug_stream_stats_t *stats);

typedef int64_t server_version_t;

server_version_t
test_framework_get_server_version_with_client (mongoc_client_t *client);
server_version_t
test_framework_get_server_version (void);
server_version_t
test_framework_str_to_version (const char *version_str);

int
test_framework_skip_if_no_dual_ip_hostname (void);

char *
test_framework_get_compressors (void);

bool
test_framework_has_compressors (void);

int
test_framework_skip_if_no_compressors (void);

int
test_framework_skip_if_compressors (void);

int
test_framework_skip_if_no_failpoint (void);

int
test_framework_skip_if_no_client_side_encryption (void);

int
test_framework_skip_if_no_aws (void);

int
test_framework_skip_if_no_setenv (void);

bool
test_framework_supports_legacy_opcodes (void);

int
test_framework_skip_if_no_legacy_opcodes (void);

int
test_framework_skip_if_no_getlasterror (void);

int
test_framework_skip_if_no_exhaust_cursors (void);

bool
test_framework_is_serverless (void);

int
test_framework_skip_if_serverless (void);

bool
test_framework_is_loadbalanced (void);

#endif
