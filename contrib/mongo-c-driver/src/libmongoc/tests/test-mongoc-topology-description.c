#include "mongoc/mongoc.h"
#include "mongoc/mongoc-set-private.h"
#include "mongoc/mongoc-client-pool-private.h"
#include "mongoc/mongoc-client-private.h"

#include "TestSuite.h"
#include "test-libmongoc.h"
#include "test-conveniences.h"

#undef MONGOC_LOG_DOMAIN
#define MONGOC_LOG_DOMAIN "topology-test"

static void
_test_has_readable_writable_server (bool pooled)
{
   mongoc_client_t *client = NULL;
   mongoc_client_pool_t *pool = NULL;
   mc_shared_tpld td;
   mongoc_read_prefs_t *prefs;
   bool r;
   bson_error_t error;
   mongoc_topology_t *topology;

   if (pooled) {
      pool = test_framework_new_default_client_pool ();
      topology = _mongoc_client_pool_get_topology (pool);
   } else {
      client = test_framework_new_default_client ();
      topology = client->topology;
   }
   td = mc_tpld_take_ref (topology);

   prefs = mongoc_read_prefs_new (MONGOC_READ_SECONDARY);
   mongoc_read_prefs_set_tags (prefs, tmp_bson ("[{'tag': 'does-not-exist'}]"));

   /* not yet connected */
   ASSERT (!mongoc_topology_description_has_writable_server (td.ptr));
   ASSERT (!mongoc_topology_description_has_readable_server (td.ptr, NULL));
   ASSERT (!mongoc_topology_description_has_readable_server (td.ptr, prefs));

   /* get a client if necessary, and trigger connection */
   if (pooled) {
      client = mongoc_client_pool_pop (pool);
   }

   r = mongoc_client_command_simple (client, "admin", tmp_bson ("{'ping': 1}"), NULL, NULL, &error);
   ASSERT_OR_PRINT (r, error);

   mc_tpld_renew_ref (&td, topology);
   ASSERT (mongoc_topology_description_has_writable_server (td.ptr));
   ASSERT (mongoc_topology_description_has_readable_server (td.ptr, NULL));

   if (test_framework_is_replset ()) {
      /* prefs still don't match any server */
      mc_tpld_renew_ref (&td, topology);
      ASSERT (!mongoc_topology_description_has_readable_server (td.ptr, prefs));
   } else {
      /* topology type single ignores read preference */
      mc_tpld_renew_ref (&td, topology);
      ASSERT (mongoc_topology_description_has_readable_server (td.ptr, prefs));
   }

   mongoc_read_prefs_destroy (prefs);

   if (pooled) {
      mongoc_client_pool_push (pool, client);
      mongoc_client_pool_destroy (pool);
   } else {
      mongoc_client_destroy (client);
   }
   mc_tpld_drop_ref (&td);
}


static void
test_has_readable_writable_server_single (void)
{
   _test_has_readable_writable_server (false);
}


static void
test_has_readable_writable_server_pooled (void)
{
   _test_has_readable_writable_server (true);
}


static const mongoc_server_description_t *
_sd_for_host (mongoc_topology_description_t *td, const char *host)
{
   const mongoc_server_description_t *sd;
   mongoc_set_t const *servers = mc_tpld_servers_const (td);

   for (size_t i = 0u; i < servers->items_len; i++) {
      sd = mongoc_set_get_item_const (servers, i);

      if (!strcmp (sd->host.host, host)) {
         return sd;
      }
   }

   return NULL;
}


static void
test_get_servers (void)
{
   mongoc_uri_t *uri;
   mongoc_topology_t *topology;
   const mongoc_server_description_t *sd_a;
   const mongoc_server_description_t *sd_c;
   mongoc_server_description_t **sds;
   mc_tpld_modification tdmod;
   size_t n;

   uri = mongoc_uri_new ("mongodb://a,b,c");
   topology = mongoc_topology_new (uri, true /* single-threaded */);
   tdmod = mc_tpld_modify_begin (topology);

   /* servers "a" and "c" are mongos, but "b" remains unknown */
   sd_a = _sd_for_host (tdmod.new_td, "a");
   mongoc_topology_description_handle_hello (
      tdmod.new_td, sd_a->id, tmp_bson ("{'ok': 1, 'msg': 'isdbgrid'}"), 100, NULL);

   sd_c = _sd_for_host (tdmod.new_td, "c");
   mongoc_topology_description_handle_hello (
      tdmod.new_td, sd_c->id, tmp_bson ("{'ok': 1, 'msg': 'isdbgrid'}"), 100, NULL);

   sds = mongoc_topology_description_get_servers (tdmod.new_td, &n);
   ASSERT_CMPSIZE_T ((size_t) 2, ==, n);

   /* we don't care which order the servers are returned */
   if (sds[0]->id == sd_a->id) {
      ASSERT_CMPSTR ("a", sds[0]->host.host);
      ASSERT_CMPSTR ("c", sds[1]->host.host);
   } else {
      ASSERT_CMPSTR ("c", sds[0]->host.host);
      ASSERT_CMPSTR ("a", sds[1]->host.host);
   }

   mongoc_server_descriptions_destroy_all (sds, n);
   mc_tpld_modify_drop (tdmod);
   mongoc_topology_destroy (topology);
   mongoc_uri_destroy (uri);
}

#define TV_1 "{ 'processId': { '$oid': 'AABBAABBAABBAABBAABBAABB' }, 'counter': 1 }"
#define TV_2 "{ 'processId': { '$oid': 'AABBAABBAABBAABBAABBAABB' }, 'counter': 2 }"

void
_topology_changed (const mongoc_apm_topology_changed_t *event)
{
   int *num_calls;

   num_calls = (int *) mongoc_apm_topology_changed_get_context (event);
   (*num_calls)++;
}

/* Regression test for CDRIVER-3753. */
static void
test_topology_version_equal (void)
{
   mongoc_uri_t *uri;
   mongoc_topology_t *topology;
   const mongoc_server_description_t *sd;
   mongoc_apm_callbacks_t *callbacks;
   int num_calls = 0;
   mc_tpld_modification tdmod;

   uri = mongoc_uri_new ("mongodb://host");
   topology = mongoc_topology_new (uri, true /* single-threaded */);
   tdmod = mc_tpld_modify_begin (topology);

   callbacks = mongoc_apm_callbacks_new ();
   mongoc_apm_set_topology_changed_cb (callbacks, _topology_changed);
   mongoc_topology_set_apm_callbacks (topology, tdmod.new_td, callbacks, &num_calls);

   sd = _sd_for_host (tdmod.new_td, "host");
   mongoc_topology_description_handle_hello (
      tdmod.new_td, sd->id, tmp_bson ("{'ok': 1, 'topologyVersion': " TV_2 " }"), 100, NULL);

   ASSERT_CMPINT (num_calls, ==, 1);

   /* The subsequent hello has a topologyVersion that compares less, so the
    * hello skips. */
   mongoc_topology_description_handle_hello (
      tdmod.new_td, sd->id, tmp_bson ("{'ok': 1, 'topologyVersion': " TV_1 " }"), 100, NULL);

   ASSERT_CMPINT (num_calls, ==, 1);

   mongoc_apm_callbacks_destroy (callbacks);
   mc_tpld_modify_drop (tdmod);
   mongoc_topology_destroy (topology);
   mongoc_uri_destroy (uri);
}

static void
test_topology_description_new_copy (void)
{
   mongoc_uri_t *uri;
   mongoc_topology_t *topology;
   mongoc_topology_description_t *td_copy;
   const mongoc_server_description_t *sd_a;
   const mongoc_server_description_t *sd_c;
   mongoc_server_description_t **sds;
   mc_tpld_modification tdmod;
   size_t n;

   uri = mongoc_uri_new ("mongodb://a,b,c");
   topology = mongoc_topology_new (uri, true /* single-threaded */);
   tdmod = mc_tpld_modify_begin (topology);

   td_copy = mongoc_topology_description_new_copy (tdmod.new_td);

   /* servers "a" and "c" are mongos, but "b" remains unknown */
   sd_a = _sd_for_host (tdmod.new_td, "a");
   mongoc_topology_description_handle_hello (
      tdmod.new_td, sd_a->id, tmp_bson ("{'ok': 1, 'msg': 'isdbgrid'}"), 100, NULL);

   sd_c = _sd_for_host (tdmod.new_td, "c");
   mongoc_topology_description_handle_hello (
      tdmod.new_td, sd_c->id, tmp_bson ("{'ok': 1, 'msg': 'isdbgrid'}"), 100, NULL);

   /* td was copied before original was updated */
   sds = mongoc_topology_description_get_servers (td_copy, &n);
   ASSERT_CMPSIZE_T ((size_t) 0, ==, n);

   mongoc_server_descriptions_destroy_all (sds, n);
   mongoc_topology_description_destroy (td_copy);

   td_copy = mongoc_topology_description_new_copy (tdmod.new_td);

   mc_tpld_modify_drop (tdmod);
   mongoc_topology_destroy (topology);
   mongoc_uri_destroy (uri);

   /* td was copied after original was updated, but before it was destroyed */
   sds = mongoc_topology_description_get_servers (td_copy, &n);
   ASSERT_CMPSIZE_T ((size_t) 2, ==, n);

   mongoc_server_descriptions_destroy_all (sds, n);
   mongoc_topology_description_destroy (td_copy);
}

/* Test that _mongoc_topology_description_clear_connection_pool increments the
 * generation.
 */
static void
test_topology_pool_clear (void)
{
   mongoc_topology_t *topology;
   mc_tpld_modification tdmod;
   mongoc_uri_t *uri;

   uri = mongoc_uri_new ("mongodb://localhost:27017,localhost:27018");
   topology = mongoc_topology_new (uri, true);
   tdmod = mc_tpld_modify_begin (topology);

   ASSERT_CMPUINT32 (0, ==, _mongoc_topology_get_connection_pool_generation (tdmod.new_td, 1, &kZeroServiceId));
   ASSERT_CMPUINT32 (0, ==, _mongoc_topology_get_connection_pool_generation (tdmod.new_td, 2, &kZeroServiceId));
   _mongoc_topology_description_clear_connection_pool (tdmod.new_td, 1, &kZeroServiceId);
   ASSERT_CMPUINT32 (1, ==, _mongoc_topology_get_connection_pool_generation (tdmod.new_td, 1, &kZeroServiceId));
   ASSERT_CMPUINT32 (0, ==, _mongoc_topology_get_connection_pool_generation (tdmod.new_td, 2, &kZeroServiceId));

   mongoc_uri_destroy (uri);
   mc_tpld_modify_drop (tdmod);
   mongoc_topology_destroy (topology);
}

static void
test_topology_pool_clear_by_serviceid (void)
{
   mongoc_topology_t *topology;
   mongoc_uri_t *uri;
   bson_oid_t oid_a;
   bson_oid_t oid_b;
   mc_tpld_modification tdmod;

   uri = mongoc_uri_new ("mongodb://localhost:27017");
   topology = mongoc_topology_new (uri, true);

   bson_oid_init_from_string (&oid_a, "AAAAAAAAAAAAAAAAAAAAAAAA");
   bson_oid_init_from_string (&oid_b, "BBBBBBBBBBBBBBBBBBBBBBBB");

   tdmod = mc_tpld_modify_begin (topology);
   ASSERT_CMPUINT32 (0, ==, _mongoc_topology_get_connection_pool_generation (tdmod.new_td, 1, &oid_a));
   ASSERT_CMPUINT32 (0, ==, _mongoc_topology_get_connection_pool_generation (tdmod.new_td, 1, &oid_b));
   _mongoc_topology_description_clear_connection_pool (tdmod.new_td, 1, &oid_a);
   ASSERT_CMPUINT32 (1, ==, _mongoc_topology_get_connection_pool_generation (tdmod.new_td, 1, &oid_a));
   ASSERT_CMPUINT32 (0, ==, _mongoc_topology_get_connection_pool_generation (tdmod.new_td, 1, &oid_b));

   mongoc_uri_destroy (uri);
   mc_tpld_modify_drop (tdmod);
   mongoc_topology_destroy (topology);
}

void
test_topology_description_install (TestSuite *suite)
{
   TestSuite_AddLive (suite, "/TopologyDescription/readable_writable/single", test_has_readable_writable_server_single);
   TestSuite_AddLive (suite, "/TopologyDescription/readable_writable/pooled", test_has_readable_writable_server_pooled);
   TestSuite_Add (suite, "/TopologyDescription/get_servers", test_get_servers);
   TestSuite_Add (suite, "/TopologyDescription/topology_version_equal", test_topology_version_equal);
   TestSuite_Add (suite, "/TopologyDescription/new_copy", test_topology_description_new_copy);
   TestSuite_Add (suite, "/TopologyDescription/pool_clear", test_topology_pool_clear);
   TestSuite_Add (suite, "/TopologyDescription/pool_clear_by_serviceid", test_topology_pool_clear_by_serviceid);
}
