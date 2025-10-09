#include <mongoc/mongoc.h>
#include <mongoc/mongoc-util-private.h>

#include "TestSuite.h"
#include "test-conveniences.h"

#undef MONGOC_LOG_DOMAIN
#define MONGOC_LOG_DOMAIN "test-util"


static void
test_command_name (void)
{
   bson_t *commands[] = {
      tmp_bson ("{'foo': 1}"),
      tmp_bson ("{'query': {'foo': 1}}"),
      tmp_bson ("{'query': {'foo': 1}, '$readPreference': 1}"),
      tmp_bson ("{'$query': {'foo': 1}}"),
      tmp_bson ("{'$query': {'foo': 1}, '$readPreference': 1}"),
      tmp_bson ("{'$readPreference': 1, '$query': {'foo': 1}}"),
   };

   size_t i;

   for (i = 0; i < sizeof (commands) / sizeof (bson_t *); i++) {
      ASSERT_CMPSTR ("foo", _mongoc_get_command_name (commands[i]));
   }
}


static void
test_rand_simple (void)
{
   int i;
   unsigned int seed = 0;
   int value, first_value;

   first_value = _mongoc_rand_simple (&seed);

   for (i = 0; i < 1000; i++) {
      value = _mongoc_rand_simple (&seed);
      if (value != first_value) {
         /* success */
         break;
      }
   }
}

static void
test_lowercase_utf8 (void)
{
   char *snowman = "\xE2\x9b\x84";
   char *letters = "aBcDe";
   char *buf = bson_malloc0 (strlen (snowman) + 1);

   mongoc_lowercase (snowman, buf);
   ASSERT_CMPSTR (snowman, buf);
   bson_free (buf);

   buf = bson_malloc0 (strlen (letters) + 1);
   mongoc_lowercase (letters, buf);
   ASSERT_CMPSTR ("abcde", buf);
   bson_free (buf);
}


static void
test_wire_server_versions (void)
{
   /* Ensure valid inclusive range. */
   ASSERT_WITH_MSG (WIRE_VERSION_MIN <= WIRE_VERSION_MAX,
                    "WIRE_VERSION_MAX (%d) must be greater than or equal to "
                    "WIRE_VERSION_MIN (%d)",
                    WIRE_VERSION_MAX,
                    WIRE_VERSION_MIN);

   /* Bumping WIRE_VERSION_MAX must be accompanied by an update to
    * `_mongoc_wire_version_to_server_version`. */
   ASSERT_WITH_MSG (strcmp (_mongoc_wire_version_to_server_version (WIRE_VERSION_MAX), "Unknown"),
                    "WIRE_VERSION_MAX must have a corresponding server version defined in "
                    "_mongoc_wire_version_to_server_version");

   /* Unlikely given the minimum version should always be a value older than the
    * maximum version, but nevertheless important to assert so warning/error
    * messages remain valid. */
   ASSERT_WITH_MSG (strcmp (_mongoc_wire_version_to_server_version (WIRE_VERSION_MIN), "Unknown"),
                    "WIRE_VERSION_MIN must have a corresponding server version defined in "
                    "_mongoc_wire_version_to_server_version");
}

static void
test_bin_to_hex (void)
{
   const char *bin = "foobar";
   const char *expect = "666f6f626172";

   char *got = bin_to_hex ((const uint8_t *) bin, (uint32_t) strlen (bin));
   ASSERT_CMPSTR (got, expect);
   bson_free (got);
}

static void
test_hex_to_bin (void)
{
   const char *hexstr = "666f6f62617200";
   const char *expect = "foobar";

   uint32_t len;
   uint8_t *got = hex_to_bin (hexstr, &len);
   ASSERT_CMPSTR ((const char *) got, expect);
   ASSERT_CMPUINT32 (len, ==, 7);
   bson_free (got);
}

void
test_util_install (TestSuite *suite)
{
   TestSuite_Add (suite, "/Util/command_name", test_command_name);
   TestSuite_Add (suite, "/Util/rand_simple", test_rand_simple);
   TestSuite_Add (suite, "/Util/lowercase_utf8", test_lowercase_utf8);
   TestSuite_Add (suite, "/Util/wire_server_versions", test_wire_server_versions);
   TestSuite_Add (suite, "/Util/bin_to_hex", test_bin_to_hex);
   TestSuite_Add (suite, "/Util/hex_to_bin", test_hex_to_bin);
}
