#include <bson/bson.h>
#include <mongoc/mongoc.h>

#include <common-thread-private.h>

#include "test-libmongoc.h"
#include "TestSuite.h"
#include "test-conveniences.h"

int
main (int argc, char *argv[])
{
   TestSuite suite;
   int ret;

   test_libmongoc_init (&suite, "libmongoc", argc, argv);

   /* libbson */

#define TEST_INSTALL(FuncName)                 \
   if (1) {                                    \
      extern void FuncName (TestSuite *suite); \
      FuncName (&suite);                       \
   } else                                      \
      ((void) 0)

   TEST_INSTALL (test_atomic_install);
   TEST_INSTALL (test_bcon_basic_install);
   TEST_INSTALL (test_bcon_extract_install);
   TEST_INSTALL (test_bson_corpus_install);
   TEST_INSTALL (test_bson_error_install);
   TEST_INSTALL (test_bson_install);
   TEST_INSTALL (test_bson_version_install);
   TEST_INSTALL (test_clock_install);
   TEST_INSTALL (test_decimal128_install);
   TEST_INSTALL (test_endian_install);
   TEST_INSTALL (test_iso8601_install);
   TEST_INSTALL (test_iter_install);
   TEST_INSTALL (test_json_install);
   TEST_INSTALL (test_oid_install);
   TEST_INSTALL (test_reader_install);
   TEST_INSTALL (test_string_install);
   TEST_INSTALL (test_utf8_install);
   TEST_INSTALL (test_value_install);
   TEST_INSTALL (test_writer_install);
   TEST_INSTALL (test_b64_install);
   TEST_INSTALL (test_bson_cmp_install);

   /* libmongoc */

   TEST_INSTALL (test_aggregate_install);
   TEST_INSTALL (test_array_install);
   TEST_INSTALL (test_async_install);
   TEST_INSTALL (test_buffer_install);
   TEST_INSTALL (test_change_stream_install);
   TEST_INSTALL (test_client_install);
   TEST_INSTALL (test_client_max_staleness_install);
   TEST_INSTALL (test_client_hedged_reads_install);
   TEST_INSTALL (test_client_pool_install);
   TEST_INSTALL (test_client_cmd_install);
   TEST_INSTALL (test_client_versioned_api_install);
   TEST_INSTALL (test_write_command_install);
   TEST_INSTALL (test_bulk_install);
   TEST_INSTALL (test_cluster_install);
   TEST_INSTALL (test_collection_install);
   TEST_INSTALL (test_collection_find_install);
   TEST_INSTALL (test_collection_find_with_opts_install);
   TEST_INSTALL (test_connection_uri_install);
   TEST_INSTALL (test_command_monitoring_install);
   TEST_INSTALL (test_cursor_install);
   TEST_INSTALL (test_database_install);
   TEST_INSTALL (test_error_install);
   TEST_INSTALL (test_exhaust_install);
   TEST_INSTALL (test_find_and_modify_install);
   TEST_INSTALL (test_gridfs_install);
   TEST_INSTALL (test_gridfs_bucket_install);
   TEST_INSTALL (test_gridfs_file_page_install);
   TEST_INSTALL (test_handshake_install);
   TEST_INSTALL (test_linux_distro_scanner_install);
   TEST_INSTALL (test_list_install);
   TEST_INSTALL (test_log_install);
   TEST_INSTALL (test_long_namespace_install);
   TEST_INSTALL (test_matcher_install);
   TEST_INSTALL (test_mongos_pinning_install);
   TEST_INSTALL (test_queue_install);
   TEST_INSTALL (test_primary_stepdown_install);
   TEST_INSTALL (test_read_concern_install);
   TEST_INSTALL (test_read_write_concern_install);
   TEST_INSTALL (test_read_prefs_install);
   TEST_INSTALL (test_retryable_writes_install);
   TEST_INSTALL (test_retryable_reads_install);
   TEST_INSTALL (test_socket_install);
   TEST_INSTALL (test_opts_install);
   TEST_INSTALL (test_topology_scanner_install);
   TEST_INSTALL (test_topology_reconcile_install);
   TEST_INSTALL (test_transactions_install);
   TEST_INSTALL (test_samples_install);
   TEST_INSTALL (test_scram_install);
   TEST_INSTALL (test_sdam_install);
   TEST_INSTALL (test_sdam_monitoring_install);
   TEST_INSTALL (test_server_selection_install);
   TEST_INSTALL (test_dns_install);
   TEST_INSTALL (test_server_selection_errors_install);
   TEST_INSTALL (test_session_install);
   TEST_INSTALL (test_set_install);
   TEST_INSTALL (test_speculative_auth_install);
   TEST_INSTALL (test_stream_install);
   TEST_INSTALL (test_thread_install);
   TEST_INSTALL (test_topology_install);
   TEST_INSTALL (test_topology_description_install);
   TEST_INSTALL (test_ts_pool_install);
   TEST_INSTALL (test_uri_install);
   TEST_INSTALL (test_usleep_install);
   TEST_INSTALL (test_util_install);
   TEST_INSTALL (test_version_install);
   TEST_INSTALL (test_with_transaction_install);
   TEST_INSTALL (test_write_concern_install);
#ifdef MONGOC_ENABLE_SSL
   TEST_INSTALL (test_stream_tls_install);
   TEST_INSTALL (test_x509_install);
   TEST_INSTALL (test_stream_tls_error_install);
   TEST_INSTALL (test_client_side_encryption_install);
#endif
#ifdef MONGOC_ENABLE_SASL_CYRUS
   TEST_INSTALL (test_cyrus_install);
#endif
   TEST_INSTALL (test_happy_eyeballs_install);
   TEST_INSTALL (test_counters_install);
   TEST_INSTALL (test_crud_install);
   TEST_INSTALL (test_mongohouse_install);
   TEST_INSTALL (test_apm_install);
   TEST_INSTALL (test_server_description_install);
   TEST_INSTALL (test_aws_install);
   TEST_INSTALL (test_streamable_hello_install);
#if defined(MONGOC_ENABLE_OCSP_OPENSSL) && OPENSSL_VERSION_NUMBER >= 0x10101000L
   TEST_INSTALL (test_ocsp_cache_install);
#endif
   TEST_INSTALL (test_interrupt_install);
   TEST_INSTALL (test_monitoring_install);
   TEST_INSTALL (test_http_install);
   TEST_INSTALL (test_install_unified);
   TEST_INSTALL (test_timeout_install);
   TEST_INSTALL (test_bson_match_install);
   TEST_INSTALL (test_bson_util_install);
   TEST_INSTALL (test_result_install);
   TEST_INSTALL (test_loadbalanced_install);
   TEST_INSTALL (test_server_stream_install);
   TEST_INSTALL (test_generation_map_install);
   TEST_INSTALL (test_shared_install);
   TEST_INSTALL (test_ssl_install);

   TEST_INSTALL (test_mcd_azure_imds_install);
   TEST_INSTALL (test_mcd_integer_install);
   TEST_INSTALL (test_mcd_rpc_install);
   TEST_INSTALL (test_service_gcp_install);
   TEST_INSTALL (test_mcd_nsinfo_install);

   ret = TestSuite_Run (&suite);

   test_libmongoc_destroy (&suite);

   return ret;
}
