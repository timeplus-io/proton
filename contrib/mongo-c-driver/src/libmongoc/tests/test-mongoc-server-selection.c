#include <mongoc/mongoc.h>

#include "json-test.h"
#include "test-libmongoc.h"


/*
 *-----------------------------------------------------------------------
 *
 * test_rtt_calculation_cb --
 *
 *       Runs the JSON tests for RTT calculation included with the
 *       Server Selection spec.
 *
 *-----------------------------------------------------------------------
 */

static void
test_rtt_calculation_cb (bson_t *test)
{
   mongoc_server_description_t *description;
   bson_iter_t iter;

   BSON_ASSERT (test);

   description = (mongoc_server_description_t *) bson_malloc0 (sizeof *description);
   mongoc_server_description_init (description, "localhost:27017", 1);

   /* parse RTT into server description */
   BSON_ASSERT (bson_iter_init_find (&iter, test, "avg_rtt_ms"));
   description->round_trip_time_msec = bson_iter_int64 (&iter);

   /* update server description with new rtt */
   BSON_ASSERT (bson_iter_init_find (&iter, test, "new_rtt_ms"));
   mongoc_server_description_update_rtt (description, bson_iter_int64 (&iter));

   /* ensure new RTT was calculated correctly */
   BSON_ASSERT (bson_iter_init_find (&iter, test, "new_avg_rtt"));
   BSON_ASSERT (description->round_trip_time_msec == bson_iter_int64 (&iter));

   mongoc_server_description_destroy (description);
}


/*
 *-----------------------------------------------------------------------
 *
 * Runner for the JSON tests for server selection.
 *
 *-----------------------------------------------------------------------
 */
static void
test_all_spec_tests (TestSuite *suite)
{
   /* RTT calculation */
   install_json_test_suite (suite, JSON_DIR, "server_selection/rtt", &test_rtt_calculation_cb);

   /* SS logic */
   install_json_test_suite (suite, JSON_DIR, "server_selection/server_selection", &test_server_selection_logic_cb);
}

void
test_server_selection_install (TestSuite *suite)
{
   test_all_spec_tests (suite);
}
