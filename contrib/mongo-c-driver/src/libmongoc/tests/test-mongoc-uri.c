#include <mongoc/mongoc.h>
#include <mongoc/mongoc-util-private.h>

#include "mongoc/mongoc-client-private.h"
#include "mongoc/mongoc-topology-private.h"
#include "mongoc/mongoc-uri-private.h"
#include "mongoc/mongoc-host-list-private.h"

#include "TestSuite.h"

#include "test-libmongoc.h"
#include "test-conveniences.h"

static void
test_mongoc_uri_new (void)
{
   const mongoc_host_list_t *hosts;
   const bson_t *options;
   const bson_t *credentials;
   const bson_t *read_prefs_tags;
   const mongoc_read_prefs_t *read_prefs;
   bson_t properties;
   mongoc_uri_t *uri;
   bson_iter_t iter;
   bson_iter_t child;

   capture_logs (true);

   /* bad uris */
   ASSERT (!mongoc_uri_new ("mongodb://"));
   ASSERT (!mongoc_uri_new ("mongodb://\x80"));
   ASSERT (!mongoc_uri_new ("mongodb://localhost/\x80"));
   ASSERT (!mongoc_uri_new ("mongodb://localhost:\x80/"));
   ASSERT (!mongoc_uri_new ("mongodb://localhost/?ipv6=\x80"));
   ASSERT (!mongoc_uri_new ("mongodb://localhost/?foo=\x80"));
   ASSERT (!mongoc_uri_new ("mongodb://localhost/?\x80=bar"));
   ASSERT (!mongoc_uri_new ("mongodb://\x80:pass@localhost"));
   ASSERT (!mongoc_uri_new ("mongodb://user:\x80@localhost"));
   ASSERT (!mongoc_uri_new ("mongodb://user%40DOMAIN.COM:password@localhost/"
                            "?" MONGOC_URI_AUTHMECHANISM "=\x80"));
   ASSERT (!mongoc_uri_new ("mongodb://user%40DOMAIN.COM:password@localhost/"
                            "?" MONGOC_URI_AUTHMECHANISM "=GSSAPI&" MONGOC_URI_AUTHMECHANISMPROPERTIES
                            "=SERVICE_NAME:\x80"));
   ASSERT (!mongoc_uri_new ("mongodb://user%40DOMAIN.COM:password@localhost/"
                            "?" MONGOC_URI_AUTHMECHANISM "=GSSAPI&" MONGOC_URI_AUTHMECHANISMPROPERTIES
                            "=\x80:mongodb"));
   ASSERT (!mongoc_uri_new ("mongodb://::"));
   ASSERT (!mongoc_uri_new ("mongodb://[::1]::27017/"));
   ASSERT (!mongoc_uri_new ("mongodb://localhost::27017"));
   ASSERT (!mongoc_uri_new ("mongodb://localhost,localhost::"));
   ASSERT (!mongoc_uri_new ("mongodb://local1,local2,local3/d?k"));
   ASSERT (!mongoc_uri_new (""));
   ASSERT (!mongoc_uri_new ("mongodb://,localhost:27017"));
   ASSERT (!mongoc_uri_new ("mongodb://localhost:27017,,b"));
   ASSERT (!mongoc_uri_new ("mongo://localhost:27017"));
   ASSERT (!mongoc_uri_new ("mongodb://localhost::27017"));
   ASSERT (!mongoc_uri_new ("mongodb://localhost::27017/"));
   ASSERT (!mongoc_uri_new ("mongodb://localhost::27017,abc"));
   ASSERT (!mongoc_uri_new ("mongodb://localhost:-1"));
   ASSERT (!mongoc_uri_new ("mongodb://localhost:65536"));
   ASSERT (!mongoc_uri_new ("mongodb://localhost:foo"));
   ASSERT (!mongoc_uri_new ("mongodb://localhost:65536/"));
   ASSERT (!mongoc_uri_new ("mongodb://localhost:0/"));
   ASSERT (!mongoc_uri_new ("mongodb://[::1%lo0]"));
   ASSERT (!mongoc_uri_new ("mongodb://[::1]:-1"));
   ASSERT (!mongoc_uri_new ("mongodb://[::1]:foo"));
   ASSERT (!mongoc_uri_new ("mongodb://[::1]:65536"));
   ASSERT (!mongoc_uri_new ("mongodb://[::1]:65536/"));
   ASSERT (!mongoc_uri_new ("mongodb://[::1]:0/"));
   ASSERT (!mongoc_uri_new ("mongodb://localhost:27017/test?replicaset="));
   ASSERT (!mongoc_uri_new ("mongodb://local1,local2/?directConnection=true"));
   ASSERT (!mongoc_uri_new ("mongodb+srv://local1/?directConnection=true"));

   uri = mongoc_uri_new ("mongodb://[::1]:27888,[::2]:27999/?ipv6=true&" MONGOC_URI_SAFE "=true");
   BSON_ASSERT (uri);
   hosts = mongoc_uri_get_hosts (uri);
   BSON_ASSERT (hosts);
   ASSERT_CMPSTR (hosts->host, "::1");
   BSON_ASSERT (hosts->port == 27888);
   ASSERT_CMPSTR (hosts->host_and_port, "[::1]:27888");
   mongoc_uri_destroy (uri);

   /* should recognize IPv6 "scope" like "::1%lo0", with % escaped  */
   uri = mongoc_uri_new ("mongodb://[::1%25lo0]");
   BSON_ASSERT (uri);
   hosts = mongoc_uri_get_hosts (uri);
   BSON_ASSERT (hosts);
   ASSERT_CMPSTR (hosts->host, "::1%lo0");
   BSON_ASSERT (hosts->port == 27017);
   ASSERT_CMPSTR (hosts->host_and_port, "[::1%lo0]:27017");
   mongoc_uri_destroy (uri);

   uri = mongoc_uri_new ("mongodb://%2Ftmp%2Fmongodb-27017.sock/?");
   ASSERT (uri);
   mongoc_uri_destroy (uri);

   /* should normalize to lowercase */
   uri = mongoc_uri_new ("mongodb://cRaZyHoStNaMe");
   BSON_ASSERT (uri);
   hosts = mongoc_uri_get_hosts (uri);
   BSON_ASSERT (hosts);
   ASSERT_CMPSTR (hosts->host, "crazyhostname");
   mongoc_uri_destroy (uri);

   uri = mongoc_uri_new ("mongodb://localhost/?");
   ASSERT (uri);
   mongoc_uri_destroy (uri);

   uri = mongoc_uri_new ("mongodb://localhost:27017/test?replicaset=foo");
   ASSERT (uri);
   hosts = mongoc_uri_get_hosts (uri);
   ASSERT (hosts);
   ASSERT (!hosts->next);
   ASSERT_CMPSTR (hosts->host, "localhost");
   ASSERT_CMPINT (hosts->port, ==, 27017);
   ASSERT_CMPSTR (hosts->host_and_port, "localhost:27017");
   ASSERT_CMPSTR (mongoc_uri_get_database (uri), "test");
   options = mongoc_uri_get_options (uri);
   ASSERT (options);
   ASSERT (bson_iter_init_find (&iter, options, "replicaset"));
   ASSERT_CMPSTR (bson_iter_utf8 (&iter, NULL), "foo");
   mongoc_uri_destroy (uri);

   uri = mongoc_uri_new ("mongodb://local1,local2:999,local3/?replicaset=foo");
   ASSERT (uri);
   hosts = mongoc_uri_get_hosts (uri);
   ASSERT (hosts);
   ASSERT (hosts->next);
   ASSERT (hosts->next->next);
   ASSERT (!hosts->next->next->next);
   ASSERT_CMPSTR (hosts->host, "local1");
   ASSERT_CMPINT (hosts->port, ==, 27017);
   ASSERT_CMPSTR (hosts->next->host, "local2");
   ASSERT_CMPINT (hosts->next->port, ==, 999);
   ASSERT_CMPSTR (hosts->next->next->host, "local3");
   ASSERT_CMPINT (hosts->next->next->port, ==, 27017);
   options = mongoc_uri_get_options (uri);
   ASSERT (options);
   ASSERT (bson_iter_init_find (&iter, options, "replicaset"));
   ASSERT_CMPSTR (bson_iter_utf8 (&iter, NULL), "foo");
   mongoc_uri_destroy (uri);

   uri = mongoc_uri_new ("mongodb://localhost:27017/"
                         "?" MONGOC_URI_READPREFERENCE "=secondaryPreferred&" MONGOC_URI_READPREFERENCETAGS
                         "=dc:ny&" MONGOC_URI_READPREFERENCETAGS "=");
   ASSERT (uri);
   read_prefs = mongoc_uri_get_read_prefs_t (uri);
   ASSERT (mongoc_read_prefs_get_mode (read_prefs) == MONGOC_READ_SECONDARY_PREFERRED);
   ASSERT (read_prefs);
   read_prefs_tags = mongoc_read_prefs_get_tags (read_prefs);
   ASSERT (read_prefs_tags);
   ASSERT_CMPINT (bson_count_keys (read_prefs_tags), ==, 2);
   ASSERT (bson_iter_init_find (&iter, read_prefs_tags, "0"));
   ASSERT (BSON_ITER_HOLDS_DOCUMENT (&iter));
   ASSERT (bson_iter_recurse (&iter, &child));
   ASSERT (bson_iter_next (&child));
   ASSERT_CMPSTR (bson_iter_key (&child), "dc");
   ASSERT_CMPSTR (bson_iter_utf8 (&child, NULL), "ny");
   ASSERT (!bson_iter_next (&child));
   ASSERT (bson_iter_next (&iter));
   ASSERT (BSON_ITER_HOLDS_DOCUMENT (&iter));
   ASSERT (bson_iter_recurse (&iter, &child));
   ASSERT (!bson_iter_next (&child));
   ASSERT (!bson_iter_next (&iter));
   mongoc_uri_destroy (uri);

   uri = mongoc_uri_new ("mongodb://localhost/?" MONGOC_URI_SAFE "=false&" MONGOC_URI_JOURNAL "=false");
   options = mongoc_uri_get_options (uri);
   ASSERT (options);
   ASSERT_CMPINT (bson_count_keys (options), ==, 2);
   ASSERT (bson_iter_init (&iter, options));
   ASSERT (bson_iter_find_case (&iter, "" MONGOC_URI_SAFE ""));
   ASSERT (BSON_ITER_HOLDS_BOOL (&iter));
   ASSERT (!bson_iter_bool (&iter));
   ASSERT (bson_iter_find_case (&iter, MONGOC_URI_JOURNAL));
   ASSERT (BSON_ITER_HOLDS_BOOL (&iter));
   ASSERT (!bson_iter_bool (&iter));
   ASSERT (!bson_iter_next (&iter));
   mongoc_uri_destroy (uri);

   uri = mongoc_uri_new ("mongodb://%2Ftmp%2Fmongodb-27017.sock/?" MONGOC_URI_TLS "=false");
   ASSERT (uri);
   ASSERT_CMPSTR (mongoc_uri_get_hosts (uri)->host, "/tmp/mongodb-27017.sock");
   mongoc_uri_destroy (uri);

   uri = mongoc_uri_new ("mongodb://%2Ftmp%2Fmongodb-27017.sock,localhost:27017/?" MONGOC_URI_TLS "=false");
   ASSERT (uri);
   ASSERT_CMPSTR (mongoc_uri_get_hosts (uri)->host, "/tmp/mongodb-27017.sock");
   ASSERT_CMPSTR (mongoc_uri_get_hosts (uri)->next->host_and_port, "localhost:27017");
   ASSERT (!mongoc_uri_get_hosts (uri)->next->next);
   mongoc_uri_destroy (uri);

   /* should assign port numbers to correct hosts */
   uri = mongoc_uri_new ("mongodb://host1,host2:30000/foo");
   ASSERT (uri);
   ASSERT_CMPSTR (mongoc_uri_get_hosts (uri)->host_and_port, "host1:27017");
   ASSERT_CMPSTR (mongoc_uri_get_hosts (uri)->next->host_and_port, "host2:30000");
   mongoc_uri_destroy (uri);

   uri = mongoc_uri_new ("mongodb://localhost:27017,%2Ftmp%2Fmongodb-27017.sock/?" MONGOC_URI_TLS "=false");
   ASSERT (uri);
   ASSERT_CMPSTR (mongoc_uri_get_hosts (uri)->host_and_port, "localhost:27017");
   ASSERT_CMPSTR (mongoc_uri_get_hosts (uri)->next->host, "/tmp/mongodb-27017.sock");
   ASSERT (!mongoc_uri_get_hosts (uri)->next->next);
   mongoc_uri_destroy (uri);

   uri = mongoc_uri_new ("mongodb://localhost/?" MONGOC_URI_HEARTBEATFREQUENCYMS "=600");
   ASSERT (uri);
   ASSERT_CMPINT32 (600, ==, mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_HEARTBEATFREQUENCYMS, 0));

   mongoc_uri_destroy (uri);

   /* heartbeat frequency too short */
   ASSERT (!mongoc_uri_new ("mongodb://localhost/?" MONGOC_URI_HEARTBEATFREQUENCYMS "=499"));

   /* should use the " MONGOC_URI_AUTHSOURCE " over db when both are specified
    */
   uri = mongoc_uri_new ("mongodb://christian:secret@localhost:27017/foo?" MONGOC_URI_AUTHSOURCE "=abcd");
   ASSERT (uri);
   ASSERT_CMPSTR (mongoc_uri_get_username (uri), "christian");
   ASSERT_CMPSTR (mongoc_uri_get_password (uri), "secret");
   ASSERT_CMPSTR (mongoc_uri_get_auth_source (uri), "abcd");
   mongoc_uri_destroy (uri);

   /* should use the default auth source and mechanism */
   uri = mongoc_uri_new ("mongodb://christian:secret@localhost:27017");
   ASSERT (uri);
   ASSERT_CMPSTR (mongoc_uri_get_auth_source (uri), "admin");
   ASSERT (!mongoc_uri_get_auth_mechanism (uri));
   mongoc_uri_destroy (uri);

   /* should use the db when no " MONGOC_URI_AUTHSOURCE " is specified */
   uri = mongoc_uri_new ("mongodb://user:password@localhost/foo");
   ASSERT (uri);
   ASSERT_CMPSTR (mongoc_uri_get_auth_source (uri), "foo");
   mongoc_uri_destroy (uri);

   /* should recognize an empty password */
   uri = mongoc_uri_new ("mongodb://samantha:@localhost");
   ASSERT (uri);
   ASSERT_CMPSTR (mongoc_uri_get_username (uri), "samantha");
   ASSERT_CMPSTR (mongoc_uri_get_password (uri), "");
   mongoc_uri_destroy (uri);

   /* should recognize no password */
   uri = mongoc_uri_new ("mongodb://christian@localhost:27017");
   ASSERT (uri);
   ASSERT_CMPSTR (mongoc_uri_get_username (uri), "christian");
   ASSERT (!mongoc_uri_get_password (uri));
   mongoc_uri_destroy (uri);

   /* should recognize a url escaped character in the username */
   uri = mongoc_uri_new ("mongodb://christian%40realm:pwd@localhost:27017");
   ASSERT (uri);
   ASSERT_CMPSTR (mongoc_uri_get_username (uri), "christian@realm");
   mongoc_uri_destroy (uri);

   /* should fail on invalid escaped characters */
   capture_logs (true);
   uri = mongoc_uri_new ("mongodb://u%ser:pwd@localhost:27017");
   ASSERT (!uri);
   ASSERT_CAPTURED_LOG ("uri", MONGOC_LOG_LEVEL_WARNING, "Invalid % escape sequence");

   uri = mongoc_uri_new ("mongodb://user:p%wd@localhost:27017");
   ASSERT (!uri);
   ASSERT_CAPTURED_LOG ("uri", MONGOC_LOG_LEVEL_WARNING, "Invalid % escape sequence");

   uri = mongoc_uri_new ("mongodb://user:pwd@local% host:27017");
   ASSERT (!uri);
   ASSERT_CAPTURED_LOG ("uri", MONGOC_LOG_LEVEL_WARNING, "Invalid % escape sequence");

   uri = mongoc_uri_new ("mongodb://christian%40realm@localhost:27017/?replicaset=%20");
   ASSERT (uri);
   options = mongoc_uri_get_options (uri);
   ASSERT (options);
   ASSERT (bson_iter_init_find (&iter, options, "replicaset"));
   ASSERT (BSON_ITER_HOLDS_UTF8 (&iter));
   ASSERT_CMPSTR (bson_iter_utf8 (&iter, NULL), " ");
   mongoc_uri_destroy (uri);

   uri = mongoc_uri_new ("mongodb://christian%40realm@[::6]:27017/?replicaset=%20");
   ASSERT (uri);
   options = mongoc_uri_get_options (uri);
   ASSERT (options);
   ASSERT (bson_iter_init_find (&iter, options, "replicaset"));
   ASSERT (BSON_ITER_HOLDS_UTF8 (&iter));
   ASSERT_CMPSTR (bson_iter_utf8 (&iter, NULL), " ");
   mongoc_uri_destroy (uri);

   /* GSSAPI-specific options */

   /* should recognize the GSSAPI mechanism, and use $external as source */
   uri = mongoc_uri_new ("mongodb://user%40DOMAIN.COM:password@localhost/"
                         "?" MONGOC_URI_AUTHMECHANISM "=GSSAPI");
   ASSERT (uri);
   ASSERT_CMPSTR (mongoc_uri_get_auth_mechanism (uri), "GSSAPI");
   /*ASSERT_CMPSTR(mongoc_uri_get_auth_source(uri), "$external");*/
   mongoc_uri_destroy (uri);

   /* use $external as source when db is specified */
   uri = mongoc_uri_new ("mongodb://user%40DOMAIN.COM:password@localhost/foo"
                         "?" MONGOC_URI_AUTHMECHANISM "=GSSAPI");
   ASSERT (uri);
   ASSERT_CMPSTR (mongoc_uri_get_auth_source (uri), "$external");
   mongoc_uri_destroy (uri);

   /* should not accept " MONGOC_URI_AUTHSOURCE " other than $external */
   ASSERT (!mongoc_uri_new ("mongodb://user%40DOMAIN.COM:password@localhost/"
                            "foo?" MONGOC_URI_AUTHMECHANISM "=GSSAPI&" MONGOC_URI_AUTHSOURCE "=bar"));

   /* should accept MONGOC_URI_AUTHMECHANISMPROPERTIES */
   uri = mongoc_uri_new ("mongodb://user%40DOMAIN.COM:password@localhost/"
                         "?" MONGOC_URI_AUTHMECHANISM "=GSSAPI&" MONGOC_URI_AUTHMECHANISMPROPERTIES
                         "=SERVICE_NAME:other,CANONICALIZE_HOST_NAME:"
                         "true");
   ASSERT (uri);
   credentials = mongoc_uri_get_credentials (uri);
   ASSERT (credentials);
   ASSERT (mongoc_uri_get_mechanism_properties (uri, &properties));
   BSON_ASSERT (bson_iter_init_find_case (&iter, &properties, "SERVICE_NAME") && BSON_ITER_HOLDS_UTF8 (&iter) &&
                (0 == strcmp (bson_iter_utf8 (&iter, NULL), "other")));
   BSON_ASSERT (bson_iter_init_find_case (&iter, &properties, "CANONICALIZE_HOST_NAME") &&
                BSON_ITER_HOLDS_UTF8 (&iter) && (0 == strcmp (bson_iter_utf8 (&iter, NULL), "true")));
   mongoc_uri_destroy (uri);

   /* reverse order of arguments to ensure parsing still succeeds */
   uri = mongoc_uri_new ("mongodb://user@localhost/?" MONGOC_URI_AUTHMECHANISMPROPERTIES
                         "=SERVICE_NAME:other&" MONGOC_URI_AUTHMECHANISM "=GSSAPI");
   ASSERT (uri);
   mongoc_uri_destroy (uri);

   /* MONGODB-CR */

   /* should recognize this mechanism */
   uri = mongoc_uri_new ("mongodb://user@localhost/?" MONGOC_URI_AUTHMECHANISM "=MONGODB-CR");
   ASSERT (uri);
   ASSERT_CMPSTR (mongoc_uri_get_auth_mechanism (uri), "MONGODB-CR");
   mongoc_uri_destroy (uri);

   /* X509 */

   /* should recognize this mechanism, and use $external as the source */
   uri = mongoc_uri_new ("mongodb://user@localhost/?" MONGOC_URI_AUTHMECHANISM "=MONGODB-X509");
   ASSERT (uri);
   ASSERT_CMPSTR (mongoc_uri_get_auth_mechanism (uri), "MONGODB-X509");
   /*ASSERT_CMPSTR(mongoc_uri_get_auth_source(uri), "$external");*/
   mongoc_uri_destroy (uri);

   /* use $external as source when db is specified */
   uri = mongoc_uri_new ("mongodb://CN%3DmyName%2COU%3DmyOrgUnit%2CO%3DmyOrg%2CL%3DmyLocality"
                         "%2CST%3DmyState%2CC%3DmyCountry@localhost/foo"
                         "?" MONGOC_URI_AUTHMECHANISM "=MONGODB-X509");
   ASSERT (uri);
   ASSERT_CMPSTR (mongoc_uri_get_auth_source (uri), "$external");
   mongoc_uri_destroy (uri);

   /* should not accept " MONGOC_URI_AUTHSOURCE " other than $external */
   ASSERT (!mongoc_uri_new ("mongodb://CN%3DmyName%2COU%3DmyOrgUnit%2CO%3DmyOrg%2CL%3DmyLocality"
                            "%2CST%3DmyState%2CC%3DmyCountry@localhost/foo"
                            "?" MONGOC_URI_AUTHMECHANISM "=MONGODB-X509&" MONGOC_URI_AUTHSOURCE "=bar"));

   /* should recognize the encoded username */
   uri = mongoc_uri_new ("mongodb://CN%3DmyName%2COU%3DmyOrgUnit%2CO%3DmyOrg%2CL%3DmyLocality"
                         "%2CST%3DmyState%2CC%3DmyCountry@localhost/?" MONGOC_URI_AUTHMECHANISM "=MONGODB-X509");
   ASSERT (uri);
   ASSERT_CMPSTR (mongoc_uri_get_username (uri), "CN=myName,OU=myOrgUnit,O=myOrg,L=myLocality,ST=myState,C=myCountry");
   mongoc_uri_destroy (uri);

   /* PLAIN */

   /* should recognize this mechanism */
   uri = mongoc_uri_new ("mongodb://user@localhost/?" MONGOC_URI_AUTHMECHANISM "=PLAIN");
   ASSERT (uri);
   ASSERT_CMPSTR (mongoc_uri_get_auth_mechanism (uri), "PLAIN");
   mongoc_uri_destroy (uri);

   /* SCRAM-SHA1 */

   /* should recognize this mechanism */
   uri = mongoc_uri_new ("mongodb://user@localhost/?" MONGOC_URI_AUTHMECHANISM "=SCRAM-SHA1");
   ASSERT (uri);
   ASSERT_CMPSTR (mongoc_uri_get_auth_mechanism (uri), "SCRAM-SHA1");
   mongoc_uri_destroy (uri);
}


static void
test_mongoc_uri_authmechanismproperties (void)
{
   mongoc_uri_t *uri;
   bson_t props;
   const bson_t *options;

   capture_logs (true);

   uri = mongoc_uri_new ("mongodb://user@localhost/?" MONGOC_URI_AUTHMECHANISM "=SCRAM-SHA1"
                         "&" MONGOC_URI_AUTHMECHANISMPROPERTIES "=a:one,b:two");
   ASSERT (uri);
   ASSERT_CMPSTR (mongoc_uri_get_auth_mechanism (uri), "SCRAM-SHA1");
   ASSERT (mongoc_uri_get_mechanism_properties (uri, &props));
   ASSERT_MATCH (&props, "{'a': 'one', 'b': 'two'}");

   ASSERT (mongoc_uri_set_auth_mechanism (uri, "MONGODB-CR"));
   ASSERT_CMPSTR (mongoc_uri_get_auth_mechanism (uri), "MONGODB-CR");

   /* prohibited */
   ASSERT (!mongoc_uri_set_option_as_utf8 (uri, MONGOC_URI_AUTHMECHANISM, "SCRAM-SHA1"));

   ASSERT (!mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_AUTHMECHANISM, 1));
   ASSERT_CAPTURED_LOG ("setting authmechanism=1",
                        MONGOC_LOG_LEVEL_WARNING,
                        "Unsupported value for \"authmechanism\": 1,"
                        " \"authmechanism\" is not an int32 option");

   ASSERT (!mongoc_uri_set_option_as_utf8 (uri, MONGOC_URI_AUTHMECHANISMPROPERTIES, "a:three"));

   ASSERT (mongoc_uri_set_mechanism_properties (uri, tmp_bson ("{'a': 'four'}")));

   ASSERT (mongoc_uri_get_mechanism_properties (uri, &props));
   ASSERT_MATCH (&props, "{'a': 'four', 'b': {'$exists': false}}");

   mongoc_uri_destroy (uri);

   /* deprecated gssapiServiceName option */
   uri = mongoc_uri_new ("mongodb://christian%40realm.cc@localhost:27017/"
                         "?" MONGOC_URI_AUTHMECHANISM "=GSSAPI&" MONGOC_URI_GSSAPISERVICENAME "=blah");
   ASSERT (uri);
   options = mongoc_uri_get_options (uri);
   ASSERT (options);
   BSON_ASSERT (0 == strcmp (mongoc_uri_get_auth_mechanism (uri), "GSSAPI"));
   BSON_ASSERT (0 == strcmp (mongoc_uri_get_username (uri), "christian@realm.cc"));
   ASSERT (mongoc_uri_get_mechanism_properties (uri, &props));
   ASSERT_MATCH (&props, "{'SERVICE_NAME': 'blah'}");
   mongoc_uri_destroy (uri);
}


static void
test_mongoc_uri_functions (void)
{
   mongoc_client_t *client;
   mongoc_uri_t *uri;
   mongoc_database_t *db;
   int32_t i;

   uri = mongoc_uri_new ("mongodb://foo:bar@localhost:27017/baz?" MONGOC_URI_AUTHSOURCE "=source");

   ASSERT_CMPSTR (mongoc_uri_get_username (uri), "foo");
   ASSERT_CMPSTR (mongoc_uri_get_password (uri), "bar");
   ASSERT_CMPSTR (mongoc_uri_get_database (uri), "baz");
   ASSERT_CMPSTR (mongoc_uri_get_auth_source (uri), "source");

   mongoc_uri_set_username (uri, "longer username that should work");
   ASSERT_CMPSTR (mongoc_uri_get_username (uri), "longer username that should work");

   mongoc_uri_set_password (uri, "longer password that should also work");
   ASSERT_CMPSTR (mongoc_uri_get_password (uri), "longer password that should also work");

   mongoc_uri_set_database (uri, "longer database that should work");
   ASSERT_CMPSTR (mongoc_uri_get_database (uri), "longer database that should work");
   ASSERT_CMPSTR (mongoc_uri_get_auth_source (uri), "source");

   mongoc_uri_set_auth_source (uri, "longer authsource that should work");
   ASSERT_CMPSTR (mongoc_uri_get_auth_source (uri), "longer authsource that should work");
   ASSERT_CMPSTR (mongoc_uri_get_database (uri), "longer database that should work");

   client = test_framework_client_new_from_uri (uri, NULL);
   mongoc_uri_destroy (uri);

   ASSERT_CMPSTR (mongoc_uri_get_username (client->uri), "longer username that should work");
   ASSERT_CMPSTR (mongoc_uri_get_password (client->uri), "longer password that should also work");
   ASSERT_CMPSTR (mongoc_uri_get_database (client->uri), "longer database that should work");
   ASSERT_CMPSTR (mongoc_uri_get_auth_source (client->uri), "longer authsource that should work");
   mongoc_client_destroy (client);


   uri = mongoc_uri_new ("mongodb://localhost/?" MONGOC_URI_SERVERSELECTIONTIMEOUTMS "=3"
                         "&" MONGOC_URI_JOURNAL "=true"
                         "&" MONGOC_URI_WTIMEOUTMS "=42"
                         "&" MONGOC_URI_CANONICALIZEHOSTNAME "=false");

   ASSERT_CMPINT (mongoc_uri_get_option_as_int32 (uri, "serverselectiontimeoutms", 18), ==, 3);
   ASSERT (mongoc_uri_set_option_as_int32 (uri, "serverselectiontimeoutms", 18));
   ASSERT_CMPINT32 (mongoc_uri_get_option_as_int32 (uri, "serverselectiontimeoutms", 19), ==, 18);

   ASSERT_CMPINT32 (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_WTIMEOUTMS, 18), ==, 42);
   ASSERT_CMPINT64 (mongoc_uri_get_option_as_int64 (uri, MONGOC_URI_WTIMEOUTMS, 18), ==, 42);
   ASSERT (mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_WTIMEOUTMS, 18));
   ASSERT_CMPINT32 (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_WTIMEOUTMS, 19), ==, 18);

   ASSERT (mongoc_uri_set_option_as_int64 (uri, MONGOC_URI_WTIMEOUTMS, 20));
   ASSERT_CMPINT64 (mongoc_uri_get_option_as_int64 (uri, MONGOC_URI_WTIMEOUTMS, 19), ==, 20);

   ASSERT (mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_HEARTBEATFREQUENCYMS, 500));

   i = mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_HEARTBEATFREQUENCYMS, 1000);

   ASSERT_CMPINT32 (i, ==, 500);

   capture_logs (true);

   /* Server Discovery and Monitoring Spec: "the driver MUST NOT permit users to
    * configure it less than minHeartbeatFrequencyMS (500ms)." */
   ASSERT (!mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_HEARTBEATFREQUENCYMS, 499));

   ASSERT_CAPTURED_LOG ("mongoc_uri_set_option_as_int32",
                        MONGOC_LOG_LEVEL_WARNING,
                        "Invalid \"heartbeatfrequencyms\" of 499: must be at least 500");

   /* socketcheckintervalms isn't set, return our fallback */
   ASSERT_CMPINT (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_SOCKETCHECKINTERVALMS, 123), ==, 123);
   ASSERT (mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_SOCKETCHECKINTERVALMS, 18));
   ASSERT_CMPINT (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_SOCKETCHECKINTERVALMS, 19), ==, 18);

   ASSERT (mongoc_uri_get_option_as_bool (uri, MONGOC_URI_JOURNAL, false));
   ASSERT (!mongoc_uri_get_option_as_bool (uri, MONGOC_URI_CANONICALIZEHOSTNAME, true));
   /* tls isn't set, return out fallback */
   ASSERT (mongoc_uri_get_option_as_bool (uri, MONGOC_URI_TLS, true));

   client = test_framework_client_new_from_uri (uri, NULL);
   mongoc_uri_destroy (uri);

   ASSERT (mongoc_uri_get_option_as_bool (client->uri, MONGOC_URI_JOURNAL, false));
   ASSERT (!mongoc_uri_get_option_as_bool (client->uri, MONGOC_URI_CANONICALIZEHOSTNAME, true));
   /* tls isn't set, return out fallback */
   ASSERT (mongoc_uri_get_option_as_bool (client->uri, MONGOC_URI_TLS, true));
   mongoc_client_destroy (client);

   uri = mongoc_uri_new ("mongodb://localhost/");
   ASSERT_CMPSTR (mongoc_uri_get_option_as_utf8 (uri, "replicaset", "default"), "default");
   ASSERT (mongoc_uri_set_option_as_utf8 (uri, "replicaset", "value"));
   ASSERT_CMPSTR (mongoc_uri_get_option_as_utf8 (uri, "replicaset", "default"), "value");

   mongoc_uri_destroy (uri);


   uri =
      mongoc_uri_new ("mongodb://localhost/?" MONGOC_URI_SOCKETTIMEOUTMS "=1&" MONGOC_URI_SOCKETCHECKINTERVALMS "=200");
   ASSERT_CMPINT (1, ==, mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_SOCKETTIMEOUTMS, 0));
   ASSERT_CMPINT (200, ==, mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_SOCKETCHECKINTERVALMS, 0));

   mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_SOCKETTIMEOUTMS, 2);
   ASSERT_CMPINT (2, ==, mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_SOCKETTIMEOUTMS, 0));

   mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_SOCKETCHECKINTERVALMS, 202);
   ASSERT_CMPINT (202, ==, mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_SOCKETCHECKINTERVALMS, 0));


   client = test_framework_client_new_from_uri (uri, NULL);
   ASSERT_CMPINT (2, ==, client->cluster.sockettimeoutms);
   ASSERT_CMPINT (202, ==, client->cluster.socketcheckintervalms);

   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);


   uri = mongoc_uri_new ("mongodb://host/dbname0");
   ASSERT_CMPSTR (mongoc_uri_get_database (uri), "dbname0");
   mongoc_uri_set_database (uri, "dbname1");
   client = test_framework_client_new_from_uri (uri, NULL);
   db = mongoc_client_get_default_database (client);
   ASSERT_CMPSTR (mongoc_database_get_name (db), "dbname1");

   mongoc_database_destroy (db);
   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);

   uri = mongoc_uri_new ("mongodb://%2Ftmp%2FMongoDB-27017.sock/");
   ASSERT_CMPSTR (mongoc_uri_get_hosts (uri)->host, "/tmp/MongoDB-27017.sock");
   ASSERT_CMPSTR (mongoc_uri_get_hosts (uri)->host_and_port, "/tmp/MongoDB-27017.sock");

   mongoc_uri_destroy (uri);

   capture_logs (true);
   uri = mongoc_uri_new ("mongodb://host/?foobar=1");
   ASSERT (uri);
   ASSERT_CAPTURED_LOG ("setting URI option foobar=1", MONGOC_LOG_LEVEL_WARNING, "Unsupported URI option \"foobar\"");

   mongoc_uri_destroy (uri);
}

static void
test_mongoc_uri_new_with_error (void)
{
   bson_error_t error = {0};
   mongoc_uri_t *uri;

   capture_logs (true);
   ASSERT (!mongoc_uri_new_with_error ("mongodb://", NULL));
   uri = mongoc_uri_new_with_error ("mongodb://localhost", NULL);
   ASSERT (uri);
   mongoc_uri_destroy (uri);

   ASSERT (!mongoc_uri_new_with_error ("mongodb://", &error));
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Invalid host string in URI");

   memset (&error, 0, sizeof (bson_error_t));
   ASSERT (!mongoc_uri_new_with_error ("mongo://localhost", &error));
   ASSERT_ERROR_CONTAINS (
      error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Invalid URI Schema, expecting 'mongodb://'");

   memset (&error, 0, sizeof (bson_error_t));
   ASSERT (!mongoc_uri_new_with_error ("mongodb://localhost/?readPreference=unknown", &error));
   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_COMMAND,
                          MONGOC_ERROR_COMMAND_INVALID_ARG,
                          "Unsupported readPreference value [readPreference=unknown]");

   memset (&error, 0, sizeof (bson_error_t));
   ASSERT (!mongoc_uri_new_with_error ("mongodb://localhost/"
                                       "?appname="
                                       "WayTooLongAppnameToBeValidSoThisShouldResultInAnErrorWayToLongAppnameToB"
                                       "eValidSoThisShouldResultInAnErrorWayToLongAppnameToBeValidSoThisShouldRe"
                                       "sultInAnError",
                                       &error));
   ASSERT_ERROR_CONTAINS (
      error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Unsupported value for \"appname\""); /* ... */

   uri = mongoc_uri_new ("mongodb://localhost");
   ASSERT (!mongoc_uri_set_option_as_utf8 (uri,
                                           MONGOC_URI_APPNAME,
                                           "WayTooLongAppnameToBeValidSoThisShouldResultInAnErrorWayToLongAppnameToB"
                                           "eValidSoThisShouldResultInAnErrorWayToLongAppnameToBeValidSoThisShouldRe"
                                           "sultInAnError"));
   mongoc_uri_destroy (uri);

   memset (&error, 0, sizeof (bson_error_t));
   ASSERT (!mongoc_uri_new_with_error ("mongodb://user%p:pass@localhost/", &error));
   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_COMMAND,
                          MONGOC_ERROR_COMMAND_INVALID_ARG,
                          "Incorrect URI escapes in username. Percent-encode "
                          "username and password according to RFC 3986");

   memset (&error, 0, sizeof (bson_error_t));
   ASSERT (!mongoc_uri_new_with_error ("mongodb://l%oc, alhost/", &error));
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Invalid host string in URI");

   memset (&error, 0, sizeof (bson_error_t));
   ASSERT (!mongoc_uri_new_with_error ("mongodb:///tmp/mongodb.sock", &error));
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Invalid host string in URI");

   memset (&error, 0, sizeof (bson_error_t));
   ASSERT (!mongoc_uri_new_with_error ("mongodb://localhost/db.na%me", &error));
   ASSERT_ERROR_CONTAINS (
      error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Invalid database name in URI");

   memset (&error, 0, sizeof (bson_error_t));
   ASSERT (!mongoc_uri_new_with_error ("mongodb://localhost/db?journal=true&w=0", &error));
   ASSERT_ERROR_CONTAINS (
      error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Journal conflicts with w value [w=0]");

   memset (&error, 0, sizeof (bson_error_t));
   ASSERT (!mongoc_uri_new_with_error ("mongodb://localhost/db?journal=true&w=-1", &error));
   ASSERT_ERROR_CONTAINS (
      error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Journal conflicts with w value [w=-1]");

   memset (&error, 0, sizeof (bson_error_t));
   ASSERT (!mongoc_uri_new_with_error ("mongodb://localhost/db?w=-5", &error));
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Unsupported w value [w=-5]");

   memset (&error, 0, sizeof (bson_error_t));
   ASSERT (!mongoc_uri_new_with_error ("mongodb://localhost/db?heartbeatfrequencyms=10", &error));
   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_COMMAND,
                          MONGOC_ERROR_COMMAND_INVALID_ARG,
                          "Invalid \"heartbeatfrequencyms\" of 10: must be at least 500");

   memset (&error, 0, sizeof (bson_error_t));
   ASSERT (!mongoc_uri_new_with_error ("mongodb://localhost/db?zlibcompressionlevel=10", &error));
   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_COMMAND,
                          MONGOC_ERROR_COMMAND_INVALID_ARG,
                          "Invalid \"zlibcompressionlevel\" of 10: must be between -1 and 9");

   memset (&error, 0, sizeof (bson_error_t));
   ASSERT (!mongoc_uri_new_with_error ("mongodb+srv://", &error));
   ASSERT_ERROR_CONTAINS (
      error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Missing service name in SRV URI");

   memset (&error, 0, sizeof (bson_error_t));
   ASSERT (!mongoc_uri_new_with_error ("mongodb+srv://%", &error));
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Invalid service name in URI");

   memset (&error, 0, sizeof (bson_error_t));
   ASSERT (!mongoc_uri_new_with_error ("mongodb+srv://x", &error));
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Invalid service name in URI");

   memset (&error, 0, sizeof (bson_error_t));
   ASSERT (!mongoc_uri_new_with_error ("mongodb+srv://x.y", &error));
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Invalid service name in URI");

   memset (&error, 0, sizeof (bson_error_t));
   ASSERT (!mongoc_uri_new_with_error ("mongodb+srv://a.b.c,d.e.f", &error));
   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_COMMAND,
                          MONGOC_ERROR_COMMAND_INVALID_ARG,
                          "Multiple service names are prohibited in an SRV URI");

   memset (&error, 0, sizeof (bson_error_t));
   ASSERT (!mongoc_uri_new_with_error ("mongodb+srv://a.b.c:8000", &error));
   ASSERT_ERROR_CONTAINS (
      error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Port numbers are prohibited in an SRV URI");
}


#undef ASSERT_SUPPRESS


static void
test_mongoc_uri_compound_setters (void)
{
   mongoc_uri_t *uri;
   mongoc_read_prefs_t *prefs;
   const mongoc_read_prefs_t *prefs_result;
   mongoc_read_concern_t *rc;
   const mongoc_read_concern_t *rc_result;
   mongoc_write_concern_t *wc;
   const mongoc_write_concern_t *wc_result;

   uri = mongoc_uri_new ("mongodb://localhost/?" MONGOC_URI_READPREFERENCE "=nearest&" MONGOC_URI_READPREFERENCETAGS
                         "=dc:ny&" MONGOC_URI_READCONCERNLEVEL "=majority&" MONGOC_URI_W "=3");

   prefs = mongoc_read_prefs_new (MONGOC_READ_SECONDARY);
   mongoc_uri_set_read_prefs_t (uri, prefs);
   prefs_result = mongoc_uri_get_read_prefs_t (uri);
   ASSERT_CMPINT (mongoc_read_prefs_get_mode (prefs_result), ==, MONGOC_READ_SECONDARY);
   ASSERT (bson_empty (mongoc_read_prefs_get_tags (prefs_result)));

   rc = mongoc_read_concern_new ();
   mongoc_read_concern_set_level (rc, "whatever");
   mongoc_uri_set_read_concern (uri, rc);
   rc_result = mongoc_uri_get_read_concern (uri);
   ASSERT_CMPSTR (mongoc_read_concern_get_level (rc_result), "whatever");

   wc = mongoc_write_concern_new ();
   mongoc_write_concern_set_w (wc, 2);
   mongoc_uri_set_write_concern (uri, wc);
   wc_result = mongoc_uri_get_write_concern (uri);
   ASSERT_CMPINT32 (mongoc_write_concern_get_w (wc_result), ==, (int32_t) 2);

   mongoc_read_prefs_destroy (prefs);
   mongoc_read_concern_destroy (rc);
   mongoc_write_concern_destroy (wc);
   mongoc_uri_destroy (uri);
}


static void
test_mongoc_host_list_from_string (void)
{
   mongoc_host_list_t host_list = {0};

   /* shouldn't be parsable */
   capture_logs (true);
   ASSERT (!_mongoc_host_list_from_string (&host_list, ":27017"));
   ASSERT_CAPTURED_LOG ("_mongoc_host_list_from_string", MONGOC_LOG_LEVEL_ERROR, "Could not parse address");
   capture_logs (true);
   ASSERT (!_mongoc_host_list_from_string (&host_list, "example.com:"));
   ASSERT_CAPTURED_LOG ("_mongoc_host_list_from_string", MONGOC_LOG_LEVEL_ERROR, "Could not parse address");
   capture_logs (true);
   ASSERT (!_mongoc_host_list_from_string (&host_list, "localhost:999999999"));
   ASSERT_CAPTURED_LOG ("_mongoc_host_list_from_string", MONGOC_LOG_LEVEL_ERROR, "Could not parse address");
   capture_logs (true);
   ASSERT (!_mongoc_host_list_from_string (&host_list, "::1234"));
   ASSERT_CAPTURED_LOG ("_mongoc_host_list_from_string", MONGOC_LOG_LEVEL_ERROR, "Could not parse address");

   capture_logs (true);
   ASSERT (!_mongoc_host_list_from_string (&host_list, "]:1234"));
   ASSERT_CAPTURED_LOG ("_mongoc_host_list_from_string", MONGOC_LOG_LEVEL_ERROR, "Could not parse address");

   capture_logs (true);
   ASSERT (!_mongoc_host_list_from_string (&host_list, "[]:1234"));
   ASSERT_CAPTURED_LOG ("_mongoc_host_list_from_string", MONGOC_LOG_LEVEL_ERROR, "Could not parse address");

   capture_logs (true);
   ASSERT (!_mongoc_host_list_from_string (&host_list, "[::1] foo"));
   ASSERT_CAPTURED_LOG ("_mongoc_host_list_from_string", MONGOC_LOG_LEVEL_ERROR, "Could not parse address");

   capture_logs (true);
   ASSERT (!_mongoc_host_list_from_string (&host_list, "[::1]extra_chars:27017"));
   ASSERT_CAPTURED_LOG ("_mongoc_host_list_from_string",
                        MONGOC_LOG_LEVEL_ERROR,
                        "If present, port should immediately follow the \"]\""
                        "in an IPv6 address");

   /* normal parsing, host and port are split, host is downcased */
   ASSERT (_mongoc_host_list_from_string (&host_list, "localHOST:27019"));
   ASSERT_CMPSTR (host_list.host_and_port, "localhost:27019");
   ASSERT_CMPSTR (host_list.host, "localhost");
   ASSERT (host_list.port == 27019);
   ASSERT (!host_list.next);

   ASSERT (_mongoc_host_list_from_string (&host_list, "localhost"));
   ASSERT_CMPSTR (host_list.host_and_port, "localhost:27017");
   ASSERT_CMPSTR (host_list.host, "localhost");
   ASSERT (host_list.port == 27017);

   ASSERT (_mongoc_host_list_from_string (&host_list, "[::1]"));
   ASSERT_CMPSTR (host_list.host_and_port, "[::1]:27017");
   ASSERT_CMPSTR (host_list.host, "::1"); /* no "[" or "]" */
   ASSERT (host_list.port == 27017);

   ASSERT (_mongoc_host_list_from_string (&host_list, "[Fe80::1]:1234"));
   ASSERT_CMPSTR (host_list.host_and_port, "[fe80::1]:1234");
   ASSERT_CMPSTR (host_list.host, "fe80::1");
   ASSERT (host_list.port == 1234);

   ASSERT (_mongoc_host_list_from_string (&host_list, "[fe80::1%lo0]:1234"));
   ASSERT_CMPSTR (host_list.host_and_port, "[fe80::1%lo0]:1234");
   ASSERT_CMPSTR (host_list.host, "fe80::1%lo0");
   ASSERT (host_list.port == 1234);

   ASSERT (_mongoc_host_list_from_string (&host_list, "[fe80::1%lo0]:1234"));
   ASSERT_CMPSTR (host_list.host_and_port, "[fe80::1%lo0]:1234");
   ASSERT_CMPSTR (host_list.host, "fe80::1%lo0");
   ASSERT (host_list.port == 1234);

   /* preserves case */
   ASSERT (_mongoc_host_list_from_string (&host_list, "/Path/to/file.sock"));
   ASSERT_CMPSTR (host_list.host_and_port, "/Path/to/file.sock");
   ASSERT_CMPSTR (host_list.host, "/Path/to/file.sock");

   /* weird cases that should still parse, without crashing */
   ASSERT (_mongoc_host_list_from_string (&host_list, "/Path/to/file.sock:1"));
   ASSERT_CMPSTR (host_list.host, "/Path/to/file.sock");
   ASSERT (host_list.family == AF_UNIX);

   ASSERT (_mongoc_host_list_from_string (&host_list, " :1234"));
   ASSERT_CMPSTR (host_list.host_and_port, " :1234");
   ASSERT_CMPSTR (host_list.host, " ");
   ASSERT (host_list.port == 1234);

   ASSERT (_mongoc_host_list_from_string (&host_list, "[:1234"));
   ASSERT_CMPSTR (host_list.host_and_port, "[:1234");
   ASSERT_CMPSTR (host_list.host, "[");
   ASSERT (host_list.port == 1234);

   ASSERT (_mongoc_host_list_from_string (&host_list, "[:]"));
   ASSERT_CMPSTR (host_list.host_and_port, "[:]:27017");
   ASSERT_CMPSTR (host_list.host, ":");
   ASSERT (host_list.port == 27017);
}


static void
test_mongoc_uri_new_for_host_port (void)
{
   mongoc_uri_t *uri;

   uri = mongoc_uri_new_for_host_port ("uber", 555);
   ASSERT (uri);
   ASSERT (!strcmp ("uber", mongoc_uri_get_hosts (uri)->host));
   ASSERT (!strcmp ("uber:555", mongoc_uri_get_hosts (uri)->host_and_port));
   ASSERT (555 == mongoc_uri_get_hosts (uri)->port);
   mongoc_uri_destroy (uri);
}

static void
test_mongoc_uri_compressors (void)
{
   mongoc_uri_t *uri;

   uri = mongoc_uri_new ("mongodb://localhost/");

   ASSERT (bson_empty (mongoc_uri_get_compressors (uri)));

#ifdef MONGOC_ENABLE_COMPRESSION_SNAPPY
   capture_logs (true);
   mongoc_uri_set_compressors (uri, "snappy,unknown");
   ASSERT (bson_has_field (mongoc_uri_get_compressors (uri), "snappy"));
   ASSERT (!bson_has_field (mongoc_uri_get_compressors (uri), "unknown"));
   ASSERT_CAPTURED_LOG ("mongoc_uri_set_compressors", MONGOC_LOG_LEVEL_WARNING, "Unsupported compressor: 'unknown'");
#endif


#ifdef MONGOC_ENABLE_COMPRESSION_SNAPPY
   capture_logs (true);
   mongoc_uri_set_compressors (uri, "snappy");
   ASSERT (bson_has_field (mongoc_uri_get_compressors (uri), "snappy"));
   ASSERT (!bson_has_field (mongoc_uri_get_compressors (uri), "unknown"));
   ASSERT_NO_CAPTURED_LOGS ("snappy uri");

   /* Overwrite the previous URI, effectively disabling snappy */
   capture_logs (true);
   mongoc_uri_set_compressors (uri, "unknown");
   ASSERT (!bson_has_field (mongoc_uri_get_compressors (uri), "snappy"));
   ASSERT (!bson_has_field (mongoc_uri_get_compressors (uri), "unknown"));
   ASSERT_CAPTURED_LOG ("mongoc_uri_set_compressors", MONGOC_LOG_LEVEL_WARNING, "Unsupported compressor: 'unknown'");
#endif

   capture_logs (true);
   mongoc_uri_set_compressors (uri, "");
   ASSERT (bson_empty (mongoc_uri_get_compressors (uri)));
   ASSERT_CAPTURED_LOG ("mongoc_uri_set_compressors", MONGOC_LOG_LEVEL_WARNING, "Unsupported compressor: ''");


   /* Disable compression */
   capture_logs (true);
   mongoc_uri_set_compressors (uri, NULL);
   ASSERT (bson_empty (mongoc_uri_get_compressors (uri)));
   ASSERT_NO_CAPTURED_LOGS ("Disable compression");


   mongoc_uri_destroy (uri);


#ifdef MONGOC_ENABLE_COMPRESSION_SNAPPY
   uri = mongoc_uri_new ("mongodb://localhost/?compressors=snappy");
   ASSERT (bson_has_field (mongoc_uri_get_compressors (uri), "snappy"));
   mongoc_uri_destroy (uri);

   capture_logs (true);
   uri = mongoc_uri_new ("mongodb://localhost/?compressors=snappy,somethingElse");
   ASSERT (bson_has_field (mongoc_uri_get_compressors (uri), "snappy"));
   ASSERT (!bson_has_field (mongoc_uri_get_compressors (uri), "somethingElse"));
   ASSERT_CAPTURED_LOG (
      "mongoc_uri_set_compressors", MONGOC_LOG_LEVEL_WARNING, "Unsupported compressor: 'somethingElse'");
   mongoc_uri_destroy (uri);
#endif


#ifdef MONGOC_ENABLE_COMPRESSION_ZLIB

#ifdef MONGOC_ENABLE_COMPRESSION_SNAPPY
   uri = mongoc_uri_new ("mongodb://localhost/?compressors=snappy,zlib");
   ASSERT (bson_has_field (mongoc_uri_get_compressors (uri), "snappy"));
   ASSERT (bson_has_field (mongoc_uri_get_compressors (uri), "zlib"));
   mongoc_uri_destroy (uri);

   uri = mongoc_uri_new ("mongodb://localhost/");
   ASSERT (mongoc_uri_set_compressors (uri, "snappy,zlib"));
   ASSERT (bson_has_field (mongoc_uri_get_compressors (uri), "snappy"));
   ASSERT (bson_has_field (mongoc_uri_get_compressors (uri), "zlib"));
   mongoc_uri_destroy (uri);

   uri = mongoc_uri_new ("mongodb://localhost/");
   ASSERT (mongoc_uri_set_compressors (uri, "zlib"));
   ASSERT (mongoc_uri_set_compressors (uri, "snappy"));
   ASSERT (bson_has_field (mongoc_uri_get_compressors (uri), "snappy"));
   ASSERT (!bson_has_field (mongoc_uri_get_compressors (uri), "zlib"));
   mongoc_uri_destroy (uri);
#endif

   uri = mongoc_uri_new ("mongodb://localhost/?compressors=zlib");
   ASSERT (bson_has_field (mongoc_uri_get_compressors (uri), "zlib"));
   mongoc_uri_destroy (uri);

   capture_logs (true);
   uri = mongoc_uri_new ("mongodb://localhost/?compressors=zlib,somethingElse");
   ASSERT (bson_has_field (mongoc_uri_get_compressors (uri), "zlib"));
   ASSERT (!bson_has_field (mongoc_uri_get_compressors (uri), "somethingElse"));
   ASSERT_CAPTURED_LOG (
      "mongoc_uri_set_compressors", MONGOC_LOG_LEVEL_WARNING, "Unsupported compressor: 'somethingElse'");
   mongoc_uri_destroy (uri);

   uri = mongoc_uri_new ("mongodb://localhost/?compressors=zlib&zlibCompressionLevel=-1");
   ASSERT (bson_has_field (mongoc_uri_get_compressors (uri), "zlib"));
   ASSERT_CMPINT32 (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_ZLIBCOMPRESSIONLEVEL, 1), ==, -1);
   mongoc_uri_destroy (uri);

   uri = mongoc_uri_new ("mongodb://localhost/?compressors=zlib&zlibCompressionLevel=9");
   ASSERT_CMPINT32 (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_ZLIBCOMPRESSIONLEVEL, 1), ==, 9);
   mongoc_uri_destroy (uri);

   capture_logs (true);
   uri = mongoc_uri_new ("mongodb://localhost/?compressors=zlib&zlibCompressionLevel=-2");
   ASSERT_CAPTURED_LOG ("mongoc_uri_set_compressors",
                        MONGOC_LOG_LEVEL_WARNING,
                        "Invalid \"zlibcompressionlevel\" of -2: must be between -1 and 9");
   mongoc_uri_destroy (uri);

   capture_logs (true);
   uri = mongoc_uri_new ("mongodb://localhost/?compressors=zlib&zlibCompressionLevel=10");
   ASSERT_CAPTURED_LOG ("mongoc_uri_set_compressors",
                        MONGOC_LOG_LEVEL_WARNING,
                        "Invalid \"zlibcompressionlevel\" of 10: must be between -1 and 9");
   mongoc_uri_destroy (uri);

#endif
}

static void
test_mongoc_uri_unescape (void)
{
#define ASSERT_URIDECODE_STR(_s, _e)        \
   do {                                     \
      char *str = mongoc_uri_unescape (_s); \
      ASSERT (!strcmp (str, _e));           \
      bson_free (str);                      \
   } while (0)
#define ASSERT_URIDECODE_FAIL(_s)                                                         \
   do {                                                                                   \
      char *str;                                                                          \
      capture_logs (true);                                                                \
      str = mongoc_uri_unescape (_s);                                                     \
      ASSERT (!str);                                                                      \
      ASSERT_CAPTURED_LOG ("uri", MONGOC_LOG_LEVEL_WARNING, "Invalid % escape sequence"); \
   } while (0)

   ASSERT_URIDECODE_STR ("", "");
   ASSERT_URIDECODE_STR ("%40", "@");
   ASSERT_URIDECODE_STR ("me%40localhost@localhost", "me@localhost@localhost");
   ASSERT_URIDECODE_STR ("%20", " ");
   ASSERT_URIDECODE_STR ("%24%21%40%2A%26%5E%21%40%2A%23%26%5E%21%40%23%2A%26"
                         "%5E%21%40%2A%23%26%5E%21%40%2A%26%23%5E%7D%7B%7D%7B"
                         "%22%22%27%7D%7B%5B%5D%3C%3E%3F",
                         "$!@*&^!@*#&^!@#*&^!@*#&^!@*&#^}{}{\"\"'}{[]<>?");

   ASSERT_URIDECODE_FAIL ("%");
   ASSERT_URIDECODE_FAIL ("%%");
   ASSERT_URIDECODE_FAIL ("%%%");
   ASSERT_URIDECODE_FAIL ("%FF");
   ASSERT_URIDECODE_FAIL ("%CC");
   ASSERT_URIDECODE_FAIL ("%00");

#undef ASSERT_URIDECODE_STR
#undef ASSERT_URIDECODE_FAIL
}


typedef struct {
   const char *uri;
   bool parses;
   mongoc_read_mode_t mode;
   bson_t *tags;
   const char *log_msg;
} read_prefs_test;


static void
test_mongoc_uri_read_prefs (void)
{
   const mongoc_read_prefs_t *rp;
   mongoc_uri_t *uri;
   const read_prefs_test *t;
   int i;

   bson_t *tags_dcny = BCON_NEW ("0", "{", "dc", "ny", "}");
   bson_t *tags_dcny_empty = BCON_NEW ("0", "{", "dc", "ny", "}", "1", "{", "}");
   bson_t *tags_dcnyusessd_dcsf_empty =
      BCON_NEW ("0", "{", "dc", "ny", "use", "ssd", "}", "1", "{", "dc", "sf", "}", "2", "{", "}");
   bson_t *tags_empty = BCON_NEW ("0", "{", "}");

   const char *conflicts = "Invalid readPreferences";

   const read_prefs_test tests[] = {
      {"mongodb://localhost/", true, MONGOC_READ_PRIMARY, NULL},
      {"mongodb://localhost/?" MONGOC_URI_READPREFERENCE "=primary", true, MONGOC_READ_PRIMARY, NULL},
      {"mongodb://localhost/?" MONGOC_URI_READPREFERENCE "=primaryPreferred",
       true,
       MONGOC_READ_PRIMARY_PREFERRED,
       NULL},
      {"mongodb://localhost/?" MONGOC_URI_READPREFERENCE "=secondary", true, MONGOC_READ_SECONDARY, NULL},
      {"mongodb://localhost/?" MONGOC_URI_READPREFERENCE "=secondaryPreferred",
       true,
       MONGOC_READ_SECONDARY_PREFERRED,
       NULL},
      {"mongodb://localhost/?" MONGOC_URI_READPREFERENCE "=nearest", true, MONGOC_READ_NEAREST, NULL},
      /* MONGOC_URI_READPREFERENCETAGS conflict with primary mode */
      {"mongodb://localhost/?" MONGOC_URI_READPREFERENCETAGS "=", false, MONGOC_READ_PRIMARY, NULL, conflicts},
      {"mongodb://localhost/?" MONGOC_URI_READPREFERENCE "=primary&" MONGOC_URI_READPREFERENCETAGS "=",
       false,
       MONGOC_READ_PRIMARY,
       NULL,
       conflicts},
      {"mongodb://localhost/"
       "?" MONGOC_URI_READPREFERENCE "=secondaryPreferred&" MONGOC_URI_READPREFERENCETAGS "=",
       true,
       MONGOC_READ_SECONDARY_PREFERRED,
       tags_empty},
      {"mongodb://localhost/"
       "?" MONGOC_URI_READPREFERENCE "=secondaryPreferred&" MONGOC_URI_READPREFERENCETAGS "=dc:ny",
       true,
       MONGOC_READ_SECONDARY_PREFERRED,
       tags_dcny},
      {"mongodb://localhost/"
       "?" MONGOC_URI_READPREFERENCE "=nearest&" MONGOC_URI_READPREFERENCETAGS "=dc:ny&" MONGOC_URI_READPREFERENCETAGS
       "=",
       true,
       MONGOC_READ_NEAREST,
       tags_dcny_empty},
      {"mongodb://localhost/"
       "?" MONGOC_URI_READPREFERENCE "=nearest&" MONGOC_URI_READPREFERENCETAGS
       "=dc:ny,use:ssd&" MONGOC_URI_READPREFERENCETAGS "=dc:sf&" MONGOC_URI_READPREFERENCETAGS "=",
       true,
       MONGOC_READ_NEAREST,
       tags_dcnyusessd_dcsf_empty},
      {"mongodb://localhost/?" MONGOC_URI_READPREFERENCE "=nearest&" MONGOC_URI_READPREFERENCETAGS "=foo",
       false,
       MONGOC_READ_NEAREST,
       NULL,
       "Unsupported value for \"" MONGOC_URI_READPREFERENCETAGS "\": \"foo\""},
      {"mongodb://localhost/?" MONGOC_URI_READPREFERENCE "=nearest&" MONGOC_URI_READPREFERENCETAGS "=foo,bar",
       false,
       MONGOC_READ_NEAREST,
       NULL,
       "Unsupported value for \"" MONGOC_URI_READPREFERENCETAGS "\": \"foo,bar\""},
      {"mongodb://localhost/?" MONGOC_URI_READPREFERENCE "=nearest&" MONGOC_URI_READPREFERENCETAGS "=1",
       false,
       MONGOC_READ_NEAREST,
       NULL,
       "Unsupported value for \"" MONGOC_URI_READPREFERENCETAGS "\": \"1\""},
      {NULL}};

   for (i = 0; tests[i].uri; i++) {
      t = &tests[i];

      capture_logs (true);
      uri = mongoc_uri_new (t->uri);
      if (t->parses) {
         BSON_ASSERT (uri);
         ASSERT_NO_CAPTURED_LOGS (t->uri);
      } else {
         BSON_ASSERT (!uri);
         if (t->log_msg) {
            ASSERT_CAPTURED_LOG (t->uri, MONGOC_LOG_LEVEL_WARNING, t->log_msg);
         }

         continue;
      }

      rp = mongoc_uri_get_read_prefs_t (uri);
      BSON_ASSERT (rp);

      BSON_ASSERT (t->mode == mongoc_read_prefs_get_mode (rp));

      if (t->tags) {
         BSON_ASSERT (bson_equal (t->tags, mongoc_read_prefs_get_tags (rp)));
      }

      mongoc_uri_destroy (uri);
   }

   bson_destroy (tags_dcny);
   bson_destroy (tags_dcny_empty);
   bson_destroy (tags_dcnyusessd_dcsf_empty);
   bson_destroy (tags_empty);
}


typedef struct {
   const char *uri;
   bool parses;
   int32_t w;
   const char *wtag;
   int64_t wtimeoutms;
   const char *log_msg;
} write_concern_test;


static void
test_mongoc_uri_write_concern (void)
{
   const mongoc_write_concern_t *wr;
   mongoc_uri_t *uri;
   const write_concern_test *t;
   int i;
   static const write_concern_test tests[] = {
      {"mongodb://localhost/?" MONGOC_URI_SAFE "=false", true, MONGOC_WRITE_CONCERN_W_UNACKNOWLEDGED},
      {"mongodb://localhost/?" MONGOC_URI_SAFE "=true", true, 1},
      {"mongodb://localhost/?" MONGOC_URI_W "=-1", true, MONGOC_WRITE_CONCERN_W_ERRORS_IGNORED},
      {"mongodb://localhost/?" MONGOC_URI_W "=0", true, MONGOC_WRITE_CONCERN_W_UNACKNOWLEDGED},
      {"mongodb://localhost/?" MONGOC_URI_W "=1", true, 1},
      {"mongodb://localhost/?" MONGOC_URI_W "=2", true, 2},
      {"mongodb://localhost/?" MONGOC_URI_W "=majority", true, MONGOC_WRITE_CONCERN_W_MAJORITY},
      {"mongodb://localhost/?" MONGOC_URI_W "=10", true, 10},
      {"mongodb://localhost/?" MONGOC_URI_W "=", true, MONGOC_WRITE_CONCERN_W_DEFAULT},
      {"mongodb://localhost/?" MONGOC_URI_W "=mytag", true, MONGOC_WRITE_CONCERN_W_TAG, "mytag"},
      {"mongodb://localhost/?" MONGOC_URI_W "=mytag&" MONGOC_URI_SAFE "=false",
       true,
       MONGOC_WRITE_CONCERN_W_TAG,
       "mytag"},
      {"mongodb://localhost/?" MONGOC_URI_W "=1&" MONGOC_URI_SAFE "=false", true, 1},
      {"mongodb://localhost/?" MONGOC_URI_JOURNAL "=true", true, MONGOC_WRITE_CONCERN_W_DEFAULT},
      {"mongodb://localhost/?" MONGOC_URI_W "=1&" MONGOC_URI_JOURNAL "=true", true, 1},
      {"mongodb://localhost/?" MONGOC_URI_W "=2&" MONGOC_URI_WTIMEOUTMS "=1000", true, 2, NULL, 1000},
      {"mongodb://localhost/?" MONGOC_URI_W "=2&" MONGOC_URI_WTIMEOUTMS "=2147483648", true, 2, NULL, 2147483648LL},
      {"mongodb://localhost/?" MONGOC_URI_W "=majority&" MONGOC_URI_WTIMEOUTMS "=1000",
       true,
       MONGOC_WRITE_CONCERN_W_MAJORITY,
       NULL,
       1000},
      {"mongodb://localhost/?" MONGOC_URI_W "=mytag&" MONGOC_URI_WTIMEOUTMS "=1000",
       true,
       MONGOC_WRITE_CONCERN_W_TAG,
       "mytag",
       1000},
      {"mongodb://localhost/?" MONGOC_URI_W "=0&" MONGOC_URI_JOURNAL "=true",
       false,
       MONGOC_WRITE_CONCERN_W_UNACKNOWLEDGED,
       NULL,
       0,
       "Journal conflicts with w value [" MONGOC_URI_W "=0]"},
      {"mongodb://localhost/?" MONGOC_URI_W "=-1&" MONGOC_URI_JOURNAL "=true",
       false,
       MONGOC_WRITE_CONCERN_W_ERRORS_IGNORED,
       NULL,
       0,
       "Journal conflicts with w value [" MONGOC_URI_W "=-1]"},
      {NULL}};

   for (i = 0; tests[i].uri; i++) {
      t = &tests[i];

      capture_logs (true);
      uri = mongoc_uri_new (t->uri);

      if (tests[i].log_msg) {
         ASSERT_CAPTURED_LOG (tests[i].uri, MONGOC_LOG_LEVEL_WARNING, tests[i].log_msg);
      } else {
         ASSERT_NO_CAPTURED_LOGS (tests[i].uri);
      }

      capture_logs (false); /* clear captured logs */

      if (t->parses) {
         BSON_ASSERT (uri);
      } else {
         BSON_ASSERT (!uri);
         continue;
      }

      wr = mongoc_uri_get_write_concern (uri);
      BSON_ASSERT (wr);

      BSON_ASSERT (t->w == mongoc_write_concern_get_w (wr));

      if (t->wtag) {
         BSON_ASSERT (0 == strcmp (t->wtag, mongoc_write_concern_get_wtag (wr)));
      }

      if (t->wtimeoutms) {
         BSON_ASSERT (t->wtimeoutms == mongoc_write_concern_get_wtimeout_int64 (wr));
      }

      mongoc_uri_destroy (uri);
   }
}

static void
test_mongoc_uri_read_concern (void)
{
   const mongoc_read_concern_t *rc;
   mongoc_uri_t *uri;

   uri = mongoc_uri_new ("mongodb://localhost/?" MONGOC_URI_READCONCERNLEVEL "=majority");
   rc = mongoc_uri_get_read_concern (uri);
   ASSERT_CMPSTR (mongoc_read_concern_get_level (rc), "majority");
   mongoc_uri_destroy (uri);

   uri = mongoc_uri_new ("mongodb://localhost/"
                         "?" MONGOC_URI_READCONCERNLEVEL "=" MONGOC_READ_CONCERN_LEVEL_MAJORITY);
   rc = mongoc_uri_get_read_concern (uri);
   ASSERT_CMPSTR (mongoc_read_concern_get_level (rc), "majority");
   mongoc_uri_destroy (uri);

   uri = mongoc_uri_new ("mongodb://localhost/"
                         "?" MONGOC_URI_READCONCERNLEVEL "=" MONGOC_READ_CONCERN_LEVEL_LINEARIZABLE);
   rc = mongoc_uri_get_read_concern (uri);
   ASSERT_CMPSTR (mongoc_read_concern_get_level (rc), "linearizable");
   mongoc_uri_destroy (uri);


   uri = mongoc_uri_new ("mongodb://localhost/?" MONGOC_URI_READCONCERNLEVEL "=local");
   rc = mongoc_uri_get_read_concern (uri);
   ASSERT_CMPSTR (mongoc_read_concern_get_level (rc), "local");
   mongoc_uri_destroy (uri);

   uri = mongoc_uri_new ("mongodb://localhost/?" MONGOC_URI_READCONCERNLEVEL "=" MONGOC_READ_CONCERN_LEVEL_LOCAL);
   rc = mongoc_uri_get_read_concern (uri);
   ASSERT_CMPSTR (mongoc_read_concern_get_level (rc), "local");
   mongoc_uri_destroy (uri);


   uri = mongoc_uri_new ("mongodb://localhost/?" MONGOC_URI_READCONCERNLEVEL "=randomstuff");
   rc = mongoc_uri_get_read_concern (uri);
   ASSERT_CMPSTR (mongoc_read_concern_get_level (rc), "randomstuff");
   mongoc_uri_destroy (uri);


   uri = mongoc_uri_new ("mongodb://localhost/");
   rc = mongoc_uri_get_read_concern (uri);
   ASSERT (mongoc_read_concern_get_level (rc) == NULL);
   mongoc_uri_destroy (uri);


   uri = mongoc_uri_new ("mongodb://localhost/?" MONGOC_URI_READCONCERNLEVEL "=");
   rc = mongoc_uri_get_read_concern (uri);
   ASSERT_CMPSTR (mongoc_read_concern_get_level (rc), "");
   mongoc_uri_destroy (uri);
}

static void
test_mongoc_uri_long_hostname (void)
{
   char *host;
   char *host_and_port;
   size_t len = BSON_HOST_NAME_MAX;
   char *uri_str;
   mongoc_uri_t *uri;

   /* hostname of exactly maximum length */
   host = bson_malloc (len + 1);
   memset (host, 'a', len);
   host[len] = '\0';
   host_and_port = bson_strdup_printf ("%s:12345", host);
   uri_str = bson_strdup_printf ("mongodb://%s", host_and_port);
   uri = mongoc_uri_new (uri_str);
   ASSERT (uri);
   ASSERT_CMPSTR (mongoc_uri_get_hosts (uri)->host_and_port, host_and_port);

   mongoc_uri_destroy (uri);
   uri = mongoc_uri_new_for_host_port (host, 12345);
   ASSERT (uri);
   ASSERT_CMPSTR (mongoc_uri_get_hosts (uri)->host_and_port, host_and_port);

   mongoc_uri_destroy (uri);
   bson_free (uri_str);
   bson_free (host_and_port);
   bson_free (host);

   /* hostname length exceeds maximum by one */
   len++;
   host = bson_malloc (len + 1);
   memset (host, 'a', len);
   host[len] = '\0';
   host_and_port = bson_strdup_printf ("%s:12345", host);
   uri_str = bson_strdup_printf ("mongodb://%s", host_and_port);

   capture_logs (true);
   ASSERT (!mongoc_uri_new (uri_str));
   ASSERT_CAPTURED_LOG ("mongoc_uri_new", MONGOC_LOG_LEVEL_ERROR, "too long");

   clear_captured_logs ();
   ASSERT (!mongoc_uri_new_for_host_port (host, 12345));
   ASSERT_CAPTURED_LOG ("mongoc_uri_new", MONGOC_LOG_LEVEL_ERROR, "too long");

   bson_free (uri_str);
   bson_free (host_and_port);
   bson_free (host);
}

static void
test_mongoc_uri_tls_ssl (const char *tls,
                         const char *tlsCertificateKeyFile,
                         const char *tlsCertificateKeyPassword,
                         const char *tlsCAFile,
                         const char *tlsAllowInvalidCertificates,
                         const char *tlsAllowInvalidHostnames)
{
   const char *tlsalt;
   char url_buffer[2048];
   mongoc_uri_t *uri;
   bson_error_t err;

   bson_snprintf (url_buffer,
                  sizeof (url_buffer),
                  "mongodb://CN=client,OU=kerneluser,O=10Gen,L=New York City,"
                  "ST=New York,C=US@ldaptest.10gen.cc/?"
                  "%s=true&authMechanism=MONGODB-X509&"
                  "%s=tests/x509gen/ldaptest-client-key-and-cert.pem&"
                  "%s=tests/x509gen/ldaptest-ca-cert.crt&"
                  "%s=true",
                  tls,
                  tlsCertificateKeyFile,
                  tlsCAFile,
                  tlsAllowInvalidHostnames);
   uri = mongoc_uri_new (url_buffer);

   ASSERT_CMPSTR (mongoc_uri_get_username (uri), "CN=client,OU=kerneluser,O=10Gen,L=New York City,ST=New York,C=US");
   ASSERT (!mongoc_uri_get_password (uri));
   ASSERT (!mongoc_uri_get_database (uri));
   ASSERT_CMPSTR (mongoc_uri_get_auth_source (uri), "$external");
   ASSERT_CMPSTR (mongoc_uri_get_auth_mechanism (uri), "MONGODB-X509");

   ASSERT_CMPSTR (mongoc_uri_get_option_as_utf8 (uri, MONGOC_URI_TLSCERTIFICATEKEYFILE, "none"),
                  "tests/x509gen/ldaptest-client-key-and-cert.pem");
   ASSERT_CMPSTR (mongoc_uri_get_option_as_utf8 (uri, MONGOC_URI_TLSCERTIFICATEKEYFILEPASSWORD, "none"), "none");
   ASSERT_CMPSTR (mongoc_uri_get_option_as_utf8 (uri, MONGOC_URI_TLSCAFILE, "none"),
                  "tests/x509gen/ldaptest-ca-cert.crt");
   ASSERT (!mongoc_uri_get_option_as_bool (uri, MONGOC_URI_TLSALLOWINVALIDCERTIFICATES, false));
   ASSERT (mongoc_uri_get_option_as_bool (uri, MONGOC_URI_TLSALLOWINVALIDHOSTNAMES, false));
   mongoc_uri_destroy (uri);


   bson_snprintf (url_buffer,
                  sizeof (url_buffer),
                  "mongodb://localhost/?%s=true&%s=key.pem&%s=ca.pem",
                  tls,
                  tlsCertificateKeyFile,
                  tlsCAFile);
   uri = mongoc_uri_new (url_buffer);

   ASSERT_CMPSTR (mongoc_uri_get_option_as_utf8 (uri, MONGOC_URI_SSLCLIENTCERTIFICATEKEYFILE, "none"), "key.pem");
   ASSERT_CMPSTR (mongoc_uri_get_option_as_utf8 (uri, MONGOC_URI_TLSCERTIFICATEKEYFILE, "none"), "key.pem");
   ASSERT_CMPSTR (mongoc_uri_get_option_as_utf8 (uri, MONGOC_URI_SSLCLIENTCERTIFICATEKEYPASSWORD, "none"), "none");
   ASSERT_CMPSTR (mongoc_uri_get_option_as_utf8 (uri, MONGOC_URI_TLSCERTIFICATEKEYFILEPASSWORD, "none"), "none");
   ASSERT_CMPSTR (mongoc_uri_get_option_as_utf8 (uri, MONGOC_URI_SSLCERTIFICATEAUTHORITYFILE, "none"), "ca.pem");
   ASSERT_CMPSTR (mongoc_uri_get_option_as_utf8 (uri, MONGOC_URI_TLSCAFILE, "none"), "ca.pem");
   ASSERT (!mongoc_uri_get_option_as_bool (uri, MONGOC_URI_TLSALLOWINVALIDCERTIFICATES, false));
   ASSERT (!mongoc_uri_get_option_as_bool (uri, MONGOC_URI_TLSALLOWINVALIDHOSTNAMES, false));
   mongoc_uri_destroy (uri);


   bson_snprintf (url_buffer, sizeof (url_buffer), "mongodb://localhost/?%s=true", tls);
   uri = mongoc_uri_new (url_buffer);

   ASSERT_CMPSTR (mongoc_uri_get_option_as_utf8 (uri, MONGOC_URI_TLSCERTIFICATEKEYFILE, "none"), "none");
   ASSERT_CMPSTR (mongoc_uri_get_option_as_utf8 (uri, MONGOC_URI_TLSCERTIFICATEKEYFILEPASSWORD, "none"), "none");
   ASSERT_CMPSTR (mongoc_uri_get_option_as_utf8 (uri, MONGOC_URI_TLSCAFILE, "none"), "none");
   ASSERT (!mongoc_uri_get_option_as_bool (uri, MONGOC_URI_TLSALLOWINVALIDCERTIFICATES, false));
   ASSERT (!mongoc_uri_get_option_as_bool (uri, MONGOC_URI_TLSALLOWINVALIDHOSTNAMES, false));
   mongoc_uri_destroy (uri);


   bson_snprintf (url_buffer,
                  sizeof (url_buffer),
                  "mongodb://localhost/?%s=true&%s=pa$$word!&%s=encrypted.pem",
                  tls,
                  tlsCertificateKeyPassword,
                  tlsCertificateKeyFile);
   uri = mongoc_uri_new (url_buffer);

   ASSERT_CMPSTR (mongoc_uri_get_option_as_utf8 (uri, MONGOC_URI_SSLCLIENTCERTIFICATEKEYFILE, "none"), "encrypted.pem");
   ASSERT_CMPSTR (mongoc_uri_get_option_as_utf8 (uri, MONGOC_URI_TLSCERTIFICATEKEYFILE, "none"), "encrypted.pem");
   ASSERT_CMPSTR (mongoc_uri_get_option_as_utf8 (uri, MONGOC_URI_SSLCLIENTCERTIFICATEKEYPASSWORD, "none"), "pa$$word!");
   ASSERT_CMPSTR (mongoc_uri_get_option_as_utf8 (uri, MONGOC_URI_TLSCERTIFICATEKEYFILEPASSWORD, "none"), "pa$$word!");
   ASSERT_CMPSTR (mongoc_uri_get_option_as_utf8 (uri, MONGOC_URI_SSLCERTIFICATEAUTHORITYFILE, "none"), "none");
   ASSERT_CMPSTR (mongoc_uri_get_option_as_utf8 (uri, MONGOC_URI_TLSCAFILE, "none"), "none");
   ASSERT (!mongoc_uri_get_option_as_bool (uri, MONGOC_URI_TLSALLOWINVALIDCERTIFICATES, false));
   ASSERT (!mongoc_uri_get_option_as_bool (uri, MONGOC_URI_TLSALLOWINVALIDHOSTNAMES, false));
   mongoc_uri_destroy (uri);


   bson_snprintf (
      url_buffer, sizeof (url_buffer), "mongodb://localhost/?%s=true&%s=true", tls, tlsAllowInvalidCertificates);
   uri = mongoc_uri_new (url_buffer);

   ASSERT_CMPSTR (mongoc_uri_get_option_as_utf8 (uri, MONGOC_URI_TLSCERTIFICATEKEYFILE, "none"), "none");
   ASSERT_CMPSTR (mongoc_uri_get_option_as_utf8 (uri, MONGOC_URI_TLSCERTIFICATEKEYFILEPASSWORD, "none"), "none");
   ASSERT_CMPSTR (mongoc_uri_get_option_as_utf8 (uri, MONGOC_URI_TLSCAFILE, "none"), "none");
   ASSERT (mongoc_uri_get_option_as_bool (uri, MONGOC_URI_SSLALLOWINVALIDCERTIFICATES, false));
   ASSERT (mongoc_uri_get_option_as_bool (uri, MONGOC_URI_TLSALLOWINVALIDCERTIFICATES, false));
   ASSERT (!mongoc_uri_get_option_as_bool (uri, MONGOC_URI_SSLALLOWINVALIDHOSTNAMES, false));
   ASSERT (!mongoc_uri_get_option_as_bool (uri, MONGOC_URI_TLSALLOWINVALIDHOSTNAMES, false));
   mongoc_uri_destroy (uri);


   bson_snprintf (url_buffer, sizeof (url_buffer), "mongodb://localhost/?%s=foo.pem", tlsCertificateKeyFile);
   uri = mongoc_uri_new (url_buffer);
   ASSERT (mongoc_uri_get_ssl (uri));
   ASSERT (mongoc_uri_get_tls (uri));
   mongoc_uri_destroy (uri);


   bson_snprintf (url_buffer, sizeof (url_buffer), "mongodb://localhost/?%s=foo.pem", tlsCAFile);
   uri = mongoc_uri_new (url_buffer);
   ASSERT (mongoc_uri_get_ssl (uri));
   ASSERT (mongoc_uri_get_tls (uri));
   mongoc_uri_destroy (uri);


   bson_snprintf (url_buffer, sizeof (url_buffer), "mongodb://localhost/?%s=true", tlsAllowInvalidCertificates);
   uri = mongoc_uri_new (url_buffer);
   ASSERT (mongoc_uri_get_ssl (uri));
   ASSERT (mongoc_uri_get_tls (uri));
   ASSERT (mongoc_uri_get_option_as_bool (uri, MONGOC_URI_SSLALLOWINVALIDCERTIFICATES, false));
   ASSERT (mongoc_uri_get_option_as_bool (uri, MONGOC_URI_TLSALLOWINVALIDCERTIFICATES, false));
   mongoc_uri_destroy (uri);


   bson_snprintf (url_buffer, sizeof (url_buffer), "mongodb://localhost/?%s=true", tlsAllowInvalidHostnames);
   uri = mongoc_uri_new (url_buffer);
   ASSERT (mongoc_uri_get_ssl (uri));
   ASSERT (mongoc_uri_get_tls (uri));
   ASSERT (mongoc_uri_get_option_as_bool (uri, MONGOC_URI_SSLALLOWINVALIDHOSTNAMES, false));
   ASSERT (mongoc_uri_get_option_as_bool (uri, MONGOC_URI_TLSALLOWINVALIDHOSTNAMES, false));
   mongoc_uri_destroy (uri);


   bson_snprintf (
      url_buffer, sizeof (url_buffer), "mongodb://localhost/?%s=false&%s=foo.pem", tls, tlsCertificateKeyFile);
   uri = mongoc_uri_new (url_buffer);
   ASSERT (!mongoc_uri_get_ssl (uri));
   ASSERT (!mongoc_uri_get_tls (uri));
   mongoc_uri_destroy (uri);


   bson_snprintf (
      url_buffer, sizeof (url_buffer), "mongodb://localhost/?%s=false&%s=foo.pem", tls, tlsCertificateKeyFile);
   uri = mongoc_uri_new (url_buffer);
   ASSERT (!mongoc_uri_get_ssl (uri));
   ASSERT (!mongoc_uri_get_tls (uri));
   mongoc_uri_destroy (uri);


   bson_snprintf (
      url_buffer, sizeof (url_buffer), "mongodb://localhost/?%s=false&%s=true", tls, tlsAllowInvalidCertificates);
   uri = mongoc_uri_new (url_buffer);
   ASSERT (!mongoc_uri_get_ssl (uri));
   ASSERT (!mongoc_uri_get_tls (uri));
   ASSERT (mongoc_uri_get_option_as_bool (uri, MONGOC_URI_SSLALLOWINVALIDCERTIFICATES, false));
   ASSERT (mongoc_uri_get_option_as_bool (uri, MONGOC_URI_TLSALLOWINVALIDCERTIFICATES, false));
   mongoc_uri_destroy (uri);


   bson_snprintf (
      url_buffer, sizeof (url_buffer), "mongodb://localhost/?%s=false&%s=false", tls, tlsAllowInvalidHostnames);
   uri = mongoc_uri_new (url_buffer);
   ASSERT (!mongoc_uri_get_ssl (uri));
   ASSERT (!mongoc_uri_get_tls (uri));
   ASSERT (!mongoc_uri_get_option_as_bool (uri, MONGOC_URI_SSLALLOWINVALIDHOSTNAMES, true));
   ASSERT (!mongoc_uri_get_option_as_bool (uri, MONGOC_URI_TLSALLOWINVALIDHOSTNAMES, true));
   mongoc_uri_destroy (uri);

   if (!strcmp (tls, "ssl")) {
      tlsalt = "tls";
   } else {
      tlsalt = "ssl";
   }

   /* Mixing options okay so long as they match */
   capture_logs (true);
   bson_snprintf (url_buffer, sizeof (url_buffer), "mongodb://localhost/?%s=true&%s=true", tls, tlsalt);
   uri = mongoc_uri_new (url_buffer);
   ASSERT (mongoc_uri_get_option_as_bool (uri, tls, false));
   ASSERT_NO_CAPTURED_LOGS (url_buffer);
   mongoc_uri_destroy (uri);

   /* Same option with different values okay, latter overrides */
   capture_logs (true);
   bson_snprintf (url_buffer, sizeof (url_buffer), "mongodb://localhost/?%s=true&%s=false", tls, tls);
   uri = mongoc_uri_new (url_buffer);
   ASSERT (!mongoc_uri_get_option_as_bool (uri, tls, true));
   if (strcmp (tls, "tls")) {
      ASSERT_CAPTURED_LOG ("option: ssl", MONGOC_LOG_LEVEL_WARNING, "Overwriting previously provided value for 'ssl'");
   } else {
      ASSERT_CAPTURED_LOG ("option: tls", MONGOC_LOG_LEVEL_WARNING, "Overwriting previously provided value for 'tls'");
   }
   mongoc_uri_destroy (uri);

   /* Mixing options not okay if values differ */
   capture_logs (false);
   bson_snprintf (url_buffer, sizeof (url_buffer), "mongodb://localhost/?%s=true&%s=false", tls, tlsalt);
   uri = mongoc_uri_new_with_error (url_buffer, &err);
   if (strcmp (tls, "tls")) {
      ASSERT_ERROR_CONTAINS (err,
                             MONGOC_ERROR_COMMAND,
                             MONGOC_ERROR_COMMAND_INVALID_ARG,
                             "Deprecated option 'ssl=true' conflicts with "
                             "canonical name 'tls=false'");
   } else {
      ASSERT_ERROR_CONTAINS (err,
                             MONGOC_ERROR_COMMAND,
                             MONGOC_ERROR_COMMAND_INVALID_ARG,
                             "Deprecated option 'ssl=false' conflicts with "
                             "canonical name 'tls=true'");
   }
   mongoc_uri_destroy (uri);

   /* No conflict appears with implicit tls=true via SRV */
   capture_logs (false);
   bson_snprintf (url_buffer, sizeof (url_buffer), "mongodb+srv://a.b.c/?%s=foo.pem", tlsCAFile);
   uri = mongoc_uri_new (url_buffer);
   ASSERT (mongoc_uri_get_option_as_bool (uri, tls, false));
   mongoc_uri_destroy (uri);

   /* Set TLS options after creating mongoc_uri_t from connection string */
   uri = mongoc_uri_new ("mongodb://localhost/");
   ASSERT (mongoc_uri_set_option_as_utf8 (uri, tlsCertificateKeyFile, "/path/to/pem"));
   ASSERT (mongoc_uri_get_tls (uri));
   mongoc_uri_destroy (uri);

   uri = mongoc_uri_new ("mongodb://localhost/");
   ASSERT (mongoc_uri_set_option_as_utf8 (uri, tlsCertificateKeyPassword, "password"));
   ASSERT (mongoc_uri_get_tls (uri));
   mongoc_uri_destroy (uri);

   uri = mongoc_uri_new ("mongodb://localhost/");
   ASSERT (mongoc_uri_set_option_as_utf8 (uri, tlsCAFile, "/path/to/pem"));
   ASSERT (mongoc_uri_get_tls (uri));
   mongoc_uri_destroy (uri);

   uri = mongoc_uri_new ("mongodb://localhost/");
   ASSERT (mongoc_uri_set_option_as_bool (uri, tlsAllowInvalidCertificates, false));
   ASSERT (mongoc_uri_get_tls (uri));
   mongoc_uri_destroy (uri);

   uri = mongoc_uri_new ("mongodb://localhost/");
   ASSERT (mongoc_uri_set_option_as_bool (uri, tlsAllowInvalidHostnames, true));
   ASSERT (mongoc_uri_get_tls (uri));
   mongoc_uri_destroy (uri);
}

static void
test_mongoc_uri_tls (void)
{
   bson_error_t err = {0};
   mongoc_uri_t *uri;

   test_mongoc_uri_tls_ssl (MONGOC_URI_TLS,
                            MONGOC_URI_TLSCERTIFICATEKEYFILE,
                            MONGOC_URI_TLSCERTIFICATEKEYFILEPASSWORD,
                            MONGOC_URI_TLSCAFILE,
                            MONGOC_URI_TLSALLOWINVALIDCERTIFICATES,
                            MONGOC_URI_TLSALLOWINVALIDHOSTNAMES);

   /* non-canonical case for tls options */
   test_mongoc_uri_tls_ssl ("tls",
                            "TlsCertificateKeyFile",
                            "tlsCertificateKeyFilePASSWORD",
                            "tlsCAFILE",
                            "TLSALLOWINVALIDCERTIFICATES",
                            "tLSaLLOWiNVALIDhOSTNAMES");

   /* non-canonical case for tls option */
   uri = mongoc_uri_new ("mongodb://localhost/?tLs=true");
   ASSERT (mongoc_uri_get_tls (uri));
   mongoc_uri_destroy (uri);

   uri = mongoc_uri_new ("mongodb://localhost/");
   ASSERT (mongoc_uri_set_option_as_bool (uri, "TLS", true));
   ASSERT (mongoc_uri_get_tls (uri));
   mongoc_uri_destroy (uri);

   /* tls-only option */
   uri = mongoc_uri_new ("mongodb://localhost/?tlsInsecure=true");
   ASSERT (mongoc_uri_get_option_as_bool (uri, MONGOC_URI_TLSINSECURE, false));
   mongoc_uri_destroy (uri);

   ASSERT (!mongoc_uri_new_with_error ("mongodb://localhost/?tlsInsecure=true&tlsAllowInvalidHostnames=false", &err));
   ASSERT_ERROR_CONTAINS (err,
                          MONGOC_ERROR_COMMAND,
                          MONGOC_ERROR_COMMAND_INVALID_ARG,
                          "tlsinsecure may not be specified with "
                          "tlsallowinvalidcertificates, tlsallowinvalidhostnames, "
                          "tlsdisableocspendpointcheck, or tlsdisablecertificaterevocationcheck");

   ASSERT (!mongoc_uri_new_with_error ("mongodb://localhost/"
                                       "?tlsInsecure=true&tlsAllowInvalidCertificates=true",
                                       &err));
   ASSERT_ERROR_CONTAINS (err,
                          MONGOC_ERROR_COMMAND,
                          MONGOC_ERROR_COMMAND_INVALID_ARG,
                          "tlsinsecure may not be specified with "
                          "tlsallowinvalidcertificates, tlsallowinvalidhostnames, "
                          "tlsdisableocspendpointcheck, or tlsdisablecertificaterevocationcheck");
}

static void
test_mongoc_uri_ssl (void)
{
   mongoc_uri_t *uri;

   test_mongoc_uri_tls_ssl (MONGOC_URI_SSL,
                            MONGOC_URI_SSLCLIENTCERTIFICATEKEYFILE,
                            MONGOC_URI_SSLCLIENTCERTIFICATEKEYPASSWORD,
                            MONGOC_URI_SSLCERTIFICATEAUTHORITYFILE,
                            MONGOC_URI_SSLALLOWINVALIDCERTIFICATES,
                            MONGOC_URI_SSLALLOWINVALIDHOSTNAMES);

   /* non-canonical case for ssl options */
   test_mongoc_uri_tls_ssl ("ssl",
                            "SslClientCertificateKeyFile",
                            "sslClientCertificateKeyPASSWORD",
                            "sslCERTIFICATEAUTHORITYFILE",
                            "SSLALLOWINVALIDCERTIFICATES",
                            "sSLaLLOWiNVALIDhOSTNAMES");

   /* non-canonical case for ssl option */
   uri = mongoc_uri_new ("mongodb://localhost/?sSl=true");
   ASSERT (mongoc_uri_get_tls (uri));
   mongoc_uri_destroy (uri);

   uri = mongoc_uri_new ("mongodb://localhost/");
   ASSERT (mongoc_uri_set_option_as_bool (uri, "SSL", true));
   ASSERT (mongoc_uri_get_tls (uri));
   mongoc_uri_destroy (uri);
}

static void
test_mongoc_uri_local_threshold_ms (void)
{
   mongoc_uri_t *uri;

   uri = mongoc_uri_new ("mongodb://localhost/");

   /* localthresholdms isn't set, return the default */
   ASSERT_CMPINT (mongoc_uri_get_local_threshold_option (uri), ==, MONGOC_TOPOLOGY_LOCAL_THRESHOLD_MS);
   ASSERT (mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_LOCALTHRESHOLDMS, 99));
   ASSERT_CMPINT (mongoc_uri_get_local_threshold_option (uri), ==, 99);

   mongoc_uri_destroy (uri);

   uri = mongoc_uri_new ("mongodb://localhost/?" MONGOC_URI_LOCALTHRESHOLDMS "=0");

   ASSERT_CMPINT (mongoc_uri_get_local_threshold_option (uri), ==, 0);
   ASSERT (mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_LOCALTHRESHOLDMS, 99));
   ASSERT_CMPINT (mongoc_uri_get_local_threshold_option (uri), ==, 99);

   mongoc_uri_destroy (uri);

   uri = mongoc_uri_new ("mongodb://localhost/?" MONGOC_URI_LOCALTHRESHOLDMS "=-1");

   /* localthresholdms is invalid, return the default */
   capture_logs (true);
   ASSERT_CMPINT (mongoc_uri_get_local_threshold_option (uri), ==, MONGOC_TOPOLOGY_LOCAL_THRESHOLD_MS);
   ASSERT_CAPTURED_LOG (
      "mongoc_uri_get_local_threshold_option", MONGOC_LOG_LEVEL_WARNING, "Invalid localThresholdMS: -1");

   mongoc_uri_destroy (uri);
}


#define INVALID(_uri, _host)                                           \
   BSON_ASSERT (!mongoc_uri_upsert_host ((_uri), (_host), 1, &error)); \
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_NAME_RESOLUTION, "must be subdomain")

#define VALID(_uri, _host) ASSERT_OR_PRINT (mongoc_uri_upsert_host ((_uri), (_host), 1, &error), error)


static void
test_mongoc_uri_srv (void)
{
   mongoc_uri_t *uri;
   bson_error_t error;

   capture_logs (true);

   ASSERT (!mongoc_uri_new ("mongodb+srv://"));
   /* requires a subdomain, domain, and TLD: "a.example.com" */
   ASSERT (!mongoc_uri_new ("mongodb+srv://foo"));
   ASSERT (!mongoc_uri_new ("mongodb+srv://foo."));
   ASSERT (!mongoc_uri_new ("mongodb+srv://.foo"));
   ASSERT (!mongoc_uri_new ("mongodb+srv://.."));
   ASSERT (!mongoc_uri_new ("mongodb+srv://.a."));
   ASSERT (!mongoc_uri_new ("mongodb+srv://.a.b.c.com"));
   ASSERT (!mongoc_uri_new ("mongodb+srv://foo\x08\x00bar"));
   ASSERT (!mongoc_uri_new ("mongodb+srv://foo%00bar"));
   ASSERT (!mongoc_uri_new ("mongodb+srv://example.com"));

   uri = mongoc_uri_new ("mongodb+srv://c.d.com");
   BSON_ASSERT (uri);
   ASSERT_CMPSTR (mongoc_uri_get_srv_hostname (uri), "c.d.com");
   BSON_ASSERT (mongoc_uri_get_hosts (uri) == NULL);

   /* tls is set to true when we use SRV */
   ASSERT_MATCH (mongoc_uri_get_options (uri), "{'tls': true}");

   /* but we can override tls */
   mongoc_uri_destroy (uri);
   uri = mongoc_uri_new ("mongodb+srv://c.d.com/?tls=false");
   BSON_ASSERT (uri);
   ASSERT_MATCH (mongoc_uri_get_options (uri), "{'tls': false}");

   INVALID (uri, "com");
   INVALID (uri, "foo.com");
   INVALID (uri, "d.com");
   INVALID (uri, "cd.com");
   VALID (uri, "c.d.com");
   VALID (uri, "bc.d.com");
   VALID (uri, "longer-string.d.com");
   INVALID (uri, ".c.d.com");
   VALID (uri, "b.c.d.com");
   INVALID (uri, ".b.c.d.com");
   INVALID (uri, "..b.c.d.com");
   VALID (uri, "a.b.c.d.com");

   mongoc_uri_destroy (uri);
   uri = mongoc_uri_new ("mongodb+srv://b.c.d.com");

   INVALID (uri, "foo.com");
   INVALID (uri, "a.b.d.com");
   INVALID (uri, "d.com");
   VALID (uri, "b.c.d.com");
   VALID (uri, "a.b.c.d.com");
   VALID (uri, "foo.a.b.c.d.com");

   mongoc_uri_destroy (uri);

   /* trailing dot is OK */
   uri = mongoc_uri_new ("mongodb+srv://service.consul.");
   BSON_ASSERT (uri);
   ASSERT_CMPSTR (mongoc_uri_get_srv_hostname (uri), "service.consul.");
   BSON_ASSERT (mongoc_uri_get_hosts (uri) == NULL);

   INVALID (uri, ".consul.");
   INVALID (uri, "service.consul");
   INVALID (uri, "a.service.consul");
   INVALID (uri, "service.a.consul");
   INVALID (uri, "a.com");
   VALID (uri, "service.consul.");
   VALID (uri, "a.service.consul.");
   VALID (uri, "a.b.service.consul.");

   mongoc_uri_destroy (uri);
}


#define PROHIBITED(_key, _value, _type, _where)                                                      \
   do {                                                                                              \
      const char *option = _key "=" #_value;                                                         \
      char *lkey = bson_strdup (_key);                                                               \
      mongoc_lowercase (lkey, lkey);                                                                 \
      mongoc_uri_parse_options (uri, option, true /* from dns */, &error);                           \
      ASSERT_ERROR_CONTAINS (                                                                        \
         error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "prohibited in TXT record"); \
      BSON_ASSERT (!bson_has_field (mongoc_uri_get_##_where (uri), lkey));                           \
      bson_free (lkey);                                                                              \
   } while (0)


static void
test_mongoc_uri_dns_options (void)
{
   mongoc_uri_t *uri;
   bson_error_t error;

   uri = mongoc_uri_new ("mongodb+srv://a.b.c");
   BSON_ASSERT (uri);

   BSON_ASSERT (!mongoc_uri_parse_options (uri, "tls=false", true /* from dsn */, &error));

   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "prohibited in TXT record");

   ASSERT_MATCH (mongoc_uri_get_options (uri), "{'tls': true}");

   /* key we want to set, value, value type, whether it's option/credential */
   PROHIBITED (MONGOC_URI_TLSALLOWINVALIDHOSTNAMES, true, bool, options);
   PROHIBITED (MONGOC_URI_TLSALLOWINVALIDCERTIFICATES, true, bool, options);
   PROHIBITED (MONGOC_URI_GSSAPISERVICENAME, malicious, utf8, credentials);

   /* the two options allowed in TXT records, case-insensitive */
   BSON_ASSERT (mongoc_uri_parse_options (uri, "authsource=db", true, NULL));
   BSON_ASSERT (mongoc_uri_parse_options (uri, "RepLIcaSET=rs", true, NULL));

   /* test that URI string overrides TXT record options */
   mongoc_uri_destroy (uri);
   uri = mongoc_uri_new ("mongodb+srv://user@a.b.c/?authSource=db1&replicaSet=rs1");

   capture_logs (true);
   /* parse_options returns true, but logs warnings */
   BSON_ASSERT (mongoc_uri_parse_options (uri, "authSource=db2&replicaSet=db2", true, NULL));
   ASSERT_CAPTURED_LOG ("parsing TXT record", MONGOC_LOG_LEVEL_WARNING, "Cannot override URI option \"authSource\"");
   ASSERT_CAPTURED_LOG ("parsing TXT record", MONGOC_LOG_LEVEL_WARNING, "Cannot override URI option \"replicaSet\"");
   capture_logs (false);
   ASSERT_MATCH (mongoc_uri_get_credentials (uri), "{'authsource': 'db1'}");
   ASSERT_MATCH (mongoc_uri_get_options (uri), "{'replicaset': 'rs1'}");

   mongoc_uri_destroy (uri);
}


/* test some invalid accesses and a crash, found with a fuzzer */
static void
test_mongoc_uri_utf8 (void)
{
   bson_error_t err;

   /* start of 3-byte character, but it's incomplete */
   BSON_ASSERT (!mongoc_uri_new_with_error ("mongodb://\xe8\x03", &err));
   ASSERT_ERROR_CONTAINS (err, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Invalid UTF-8 in URI");

   /* start of 6-byte CESU-8 character, but it's incomplete */
   BSON_ASSERT (!mongoc_uri_new_with_error ("mongodb://\xfa", &err));
   ASSERT_ERROR_CONTAINS (err, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Invalid UTF-8 in URI");


   /* "a<NIL>z" with NIL expressed as two-byte sequence */
   BSON_ASSERT (!mongoc_uri_new_with_error ("mongodb://a\xc0\x80z", &err));
   ASSERT_ERROR_CONTAINS (err, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Invalid UTF-8 in URI");
}


/* test behavior on duplicate values for an options. */
static void
test_mongoc_uri_duplicates (void)
{
   mongoc_uri_t *uri = NULL;
   bson_error_t err;
   const char *str;
   const mongoc_write_concern_t *wc;
   const mongoc_read_concern_t *rc;
   const bson_t *bson;
   const mongoc_read_prefs_t *rp;
   bson_iter_t iter = {0};

#define RECREATE_URI(opts)                                                               \
   if (1) {                                                                              \
      mongoc_uri_destroy (uri);                                                          \
      uri = mongoc_uri_new_with_error ("mongodb://user:pwd@localhost/test?" opts, &err); \
      ASSERT_OR_PRINT (uri, err);                                                        \
   } else                                                                                \
      (void) 0

#define ASSERT_LOG_DUPE(opt) \
   ASSERT_CAPTURED_LOG ("option: " opt, MONGOC_LOG_LEVEL_WARNING, "Overwriting previously provided value for '" opt "'")

/* iterate iter to key, and check that no other occurrences exist. */
#define BSON_ITER_UNIQUE(key)                                             \
   do {                                                                   \
      bson_iter_t tmp;                                                    \
      BSON_ASSERT (bson_iter_init_find (&iter, bson, key));               \
      tmp = iter;                                                         \
      while (bson_iter_next (&tmp)) {                                     \
         if (strcmp (bson_iter_key (&tmp), key) == 0) {                   \
            ASSERT_WITH_MSG (false, "bson has duplicate keys for: " key); \
         }                                                                \
      }                                                                   \
   } while (0)

   capture_logs (true);

   /* test all URI options, in the order they are defined in mongoc-uri.h. */
   RECREATE_URI (MONGOC_URI_APPNAME "=a&" MONGOC_URI_APPNAME "=b");
   ASSERT_LOG_DUPE (MONGOC_URI_APPNAME);
   str = mongoc_uri_get_appname (uri);
   BSON_ASSERT (strcmp (str, "b") == 0);

   RECREATE_URI (MONGOC_URI_AUTHMECHANISM "=a&" MONGOC_URI_AUTHMECHANISM "=b");
   ASSERT_LOG_DUPE (MONGOC_URI_AUTHMECHANISM);
   bson = mongoc_uri_get_credentials (uri);
   BSON_ITER_UNIQUE (MONGOC_URI_AUTHMECHANISM);
   BSON_ASSERT (strcmp (bson_iter_utf8 (&iter, NULL), "b") == 0);

   RECREATE_URI (MONGOC_URI_AUTHMECHANISMPROPERTIES "=a:x&" MONGOC_URI_AUTHMECHANISMPROPERTIES "=b:y");
   ASSERT_LOG_DUPE (MONGOC_URI_AUTHMECHANISMPROPERTIES);
   bson = mongoc_uri_get_credentials (uri);
   BSON_ASSERT (bson_compare (bson, tmp_bson ("{'authmechanismproperties': {'b': 'y' }}")) == 0);

   RECREATE_URI (MONGOC_URI_AUTHSOURCE "=a&" MONGOC_URI_AUTHSOURCE "=b");
   ASSERT_LOG_DUPE (MONGOC_URI_AUTHSOURCE);
   str = mongoc_uri_get_auth_source (uri);
   BSON_ASSERT (strcmp (str, "b") == 0);

   RECREATE_URI (MONGOC_URI_CANONICALIZEHOSTNAME "=false&" MONGOC_URI_CANONICALIZEHOSTNAME "=true");
   ASSERT_LOG_DUPE (MONGOC_URI_CANONICALIZEHOSTNAME);
   BSON_ASSERT (mongoc_uri_get_option_as_bool (uri, MONGOC_URI_CANONICALIZEHOSTNAME, false));

   RECREATE_URI (MONGOC_URI_CONNECTTIMEOUTMS "=1&" MONGOC_URI_CONNECTTIMEOUTMS "=2");
   ASSERT_LOG_DUPE (MONGOC_URI_CONNECTTIMEOUTMS);
   BSON_ASSERT (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_CONNECTTIMEOUTMS, 0) == 2);

#if defined(MONGOC_ENABLE_COMPRESSION_SNAPPY) && defined(MONGOC_ENABLE_COMPRESSION_ZLIB)
   RECREATE_URI (MONGOC_URI_COMPRESSORS "=snappy&" MONGOC_URI_COMPRESSORS "=zlib");
   ASSERT_LOG_DUPE (MONGOC_URI_COMPRESSORS);
   bson = mongoc_uri_get_compressors (uri);
   BSON_ASSERT (bson_compare (bson, tmp_bson ("{'zlib': 'yes'}")) == 0);
#endif

   /* exception: GSSAPISERVICENAME does not overwrite. */
   RECREATE_URI (MONGOC_URI_GSSAPISERVICENAME "=a&" MONGOC_URI_GSSAPISERVICENAME "=b");
   ASSERT_CAPTURED_LOG ("option: " MONGOC_URI_GSSAPISERVICENAME,
                        MONGOC_LOG_LEVEL_WARNING,
                        "Overwriting previously provided value for 'gssapiservicename'");
   bson = mongoc_uri_get_credentials (uri);
   BSON_ASSERT (bson_compare (bson, tmp_bson ("{'authmechanismproperties': {'SERVICE_NAME': 'b' }}")) == 0);

   RECREATE_URI (MONGOC_URI_HEARTBEATFREQUENCYMS "=500&" MONGOC_URI_HEARTBEATFREQUENCYMS "=501");
   ASSERT_LOG_DUPE (MONGOC_URI_HEARTBEATFREQUENCYMS);
   BSON_ASSERT (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_HEARTBEATFREQUENCYMS, 0) == 501);

   RECREATE_URI (MONGOC_URI_JOURNAL "=false&" MONGOC_URI_JOURNAL "=true");
   ASSERT_LOG_DUPE (MONGOC_URI_JOURNAL);
   BSON_ASSERT (mongoc_uri_get_option_as_bool (uri, MONGOC_URI_JOURNAL, false));

   RECREATE_URI (MONGOC_URI_LOCALTHRESHOLDMS "=1&" MONGOC_URI_LOCALTHRESHOLDMS "=2");
   ASSERT_LOG_DUPE (MONGOC_URI_LOCALTHRESHOLDMS);
   BSON_ASSERT (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_LOCALTHRESHOLDMS, 0) == 2);

   RECREATE_URI (MONGOC_URI_MAXIDLETIMEMS "=1&" MONGOC_URI_MAXIDLETIMEMS "=2");
   ASSERT_LOG_DUPE (MONGOC_URI_MAXIDLETIMEMS);
   BSON_ASSERT (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_MAXIDLETIMEMS, 0) == 2);

   RECREATE_URI (MONGOC_URI_MAXPOOLSIZE "=1&" MONGOC_URI_MAXPOOLSIZE "=2");
   ASSERT_LOG_DUPE (MONGOC_URI_MAXPOOLSIZE);
   BSON_ASSERT (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_MAXPOOLSIZE, 0) == 2);

   RECREATE_URI (MONGOC_URI_READPREFERENCE "=secondary&" MONGOC_URI_MAXSTALENESSSECONDS
                                           "=1&" MONGOC_URI_MAXSTALENESSSECONDS "=2");
   ASSERT_LOG_DUPE (MONGOC_URI_MAXSTALENESSSECONDS);
   BSON_ASSERT (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_MAXSTALENESSSECONDS, 0) == 2);

   RECREATE_URI (MONGOC_URI_MINPOOLSIZE "=1&" MONGOC_URI_MINPOOLSIZE "=2");
   ASSERT_LOG_DUPE (MONGOC_URI_MINPOOLSIZE);
   BSON_ASSERT (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_MINPOOLSIZE, 0) == 2);

   RECREATE_URI (MONGOC_URI_READCONCERNLEVEL "=local&" MONGOC_URI_READCONCERNLEVEL "=majority");
   ASSERT_LOG_DUPE (MONGOC_URI_READCONCERNLEVEL);
   rc = mongoc_uri_get_read_concern (uri);
   BSON_ASSERT (strcmp (mongoc_read_concern_get_level (rc), "majority") == 0);

   RECREATE_URI (MONGOC_URI_READPREFERENCE "=secondary&" MONGOC_URI_READPREFERENCE "=primary");
   ASSERT_LOG_DUPE (MONGOC_URI_READPREFERENCE);
   rp = mongoc_uri_get_read_prefs_t (uri);
   BSON_ASSERT (mongoc_read_prefs_get_mode (rp) == MONGOC_READ_PRIMARY);

   /* exception: read preference tags get appended. */
   RECREATE_URI (MONGOC_URI_READPREFERENCE "=secondary&" MONGOC_URI_READPREFERENCETAGS
                                           "=a:x&" MONGOC_URI_READPREFERENCETAGS "=b:y");
   bson = mongoc_uri_get_read_prefs (uri);
   BSON_ASSERT (bson_compare (bson, tmp_bson ("{'0': {'a': 'x'}, '1': {'b': 'y'}}")) == 0);

   RECREATE_URI (MONGOC_URI_REPLICASET "=a&" MONGOC_URI_REPLICASET "=b");
   ASSERT_LOG_DUPE (MONGOC_URI_REPLICASET);
   str = mongoc_uri_get_replica_set (uri);
   BSON_ASSERT (strcmp (str, "b") == 0);

   RECREATE_URI (MONGOC_URI_RETRYREADS "=false&" MONGOC_URI_RETRYREADS "=true");
   ASSERT_LOG_DUPE (MONGOC_URI_RETRYREADS);
   BSON_ASSERT (mongoc_uri_get_option_as_bool (uri, MONGOC_URI_RETRYREADS, false));

   RECREATE_URI (MONGOC_URI_RETRYWRITES "=false&" MONGOC_URI_RETRYWRITES "=true");
   ASSERT_LOG_DUPE (MONGOC_URI_RETRYWRITES);
   BSON_ASSERT (mongoc_uri_get_option_as_bool (uri, MONGOC_URI_RETRYWRITES, false));

   RECREATE_URI (MONGOC_URI_SAFE "=false&" MONGOC_URI_SAFE "=true");
   ASSERT_LOG_DUPE (MONGOC_URI_SAFE);
   BSON_ASSERT (mongoc_uri_get_option_as_bool (uri, MONGOC_URI_SAFE, false));

   RECREATE_URI (MONGOC_URI_SERVERSELECTIONTIMEOUTMS "=1&" MONGOC_URI_SERVERSELECTIONTIMEOUTMS "=2");
   ASSERT_LOG_DUPE (MONGOC_URI_SERVERSELECTIONTIMEOUTMS);
   BSON_ASSERT (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_SERVERSELECTIONTIMEOUTMS, 0) == 2);

   RECREATE_URI (MONGOC_URI_SERVERSELECTIONTRYONCE "=false&" MONGOC_URI_SERVERSELECTIONTRYONCE "=true");
   ASSERT_LOG_DUPE (MONGOC_URI_SERVERSELECTIONTRYONCE);
   BSON_ASSERT (mongoc_uri_get_option_as_bool (uri, MONGOC_URI_SERVERSELECTIONTRYONCE, false));

   RECREATE_URI (MONGOC_URI_SOCKETCHECKINTERVALMS "=1&" MONGOC_URI_SOCKETCHECKINTERVALMS "=2");
   ASSERT_LOG_DUPE (MONGOC_URI_SOCKETCHECKINTERVALMS);
   BSON_ASSERT (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_SOCKETCHECKINTERVALMS, 0) == 2);

   RECREATE_URI (MONGOC_URI_SOCKETTIMEOUTMS "=1&" MONGOC_URI_SOCKETTIMEOUTMS "=2");
   ASSERT_LOG_DUPE (MONGOC_URI_SOCKETTIMEOUTMS);
   BSON_ASSERT (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_SOCKETTIMEOUTMS, 0) == 2);

   RECREATE_URI (MONGOC_URI_TLS "=false&" MONGOC_URI_TLS "=true");
   ASSERT_LOG_DUPE (MONGOC_URI_TLS);
   BSON_ASSERT (mongoc_uri_get_tls (uri));

   RECREATE_URI (MONGOC_URI_TLSCERTIFICATEKEYFILE "=a&" MONGOC_URI_TLSCERTIFICATEKEYFILE "=b");
   ASSERT_LOG_DUPE (MONGOC_URI_TLSCERTIFICATEKEYFILE);
   str = mongoc_uri_get_option_as_utf8 (uri, MONGOC_URI_TLSCERTIFICATEKEYFILE, "");
   BSON_ASSERT (strcmp (str, "b") == 0);

   RECREATE_URI (MONGOC_URI_TLSCERTIFICATEKEYFILEPASSWORD "=a&" MONGOC_URI_TLSCERTIFICATEKEYFILEPASSWORD "=b");
   ASSERT_LOG_DUPE (MONGOC_URI_TLSCERTIFICATEKEYFILEPASSWORD);
   str = mongoc_uri_get_option_as_utf8 (uri, MONGOC_URI_TLSCERTIFICATEKEYFILEPASSWORD, "");
   BSON_ASSERT (strcmp (str, "b") == 0);

   RECREATE_URI (MONGOC_URI_TLSCAFILE "=a&" MONGOC_URI_TLSCAFILE "=b");
   ASSERT_LOG_DUPE (MONGOC_URI_TLSCAFILE);
   str = mongoc_uri_get_option_as_utf8 (uri, MONGOC_URI_TLSCAFILE, "");
   BSON_ASSERT (strcmp (str, "b") == 0);

   RECREATE_URI (MONGOC_URI_TLSALLOWINVALIDCERTIFICATES "=false&" MONGOC_URI_TLSALLOWINVALIDCERTIFICATES "=true");
   ASSERT_LOG_DUPE (MONGOC_URI_TLSALLOWINVALIDCERTIFICATES);
   BSON_ASSERT (mongoc_uri_get_option_as_bool (uri, MONGOC_URI_TLSALLOWINVALIDCERTIFICATES, false));

   RECREATE_URI (MONGOC_URI_W "=1&" MONGOC_URI_W "=0");
   ASSERT_LOG_DUPE (MONGOC_URI_W);
   wc = mongoc_uri_get_write_concern (uri);
   BSON_ASSERT (mongoc_write_concern_get_w (wc) == 0);

   /* exception: a string write concern takes precedence over an int */
   RECREATE_URI (MONGOC_URI_W "=majority&" MONGOC_URI_W "=0");
   ASSERT_LOG_DUPE (MONGOC_URI_W);
   wc = mongoc_uri_get_write_concern (uri);
   BSON_ASSERT (mongoc_write_concern_get_w (wc) == MONGOC_WRITE_CONCERN_W_MAJORITY);

   RECREATE_URI (MONGOC_URI_WAITQUEUEMULTIPLE "=1&" MONGOC_URI_WAITQUEUEMULTIPLE "=2");
   ASSERT_LOG_DUPE (MONGOC_URI_WAITQUEUEMULTIPLE);
   BSON_ASSERT (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_WAITQUEUEMULTIPLE, 0) == 2);

   RECREATE_URI (MONGOC_URI_WAITQUEUETIMEOUTMS "=1&" MONGOC_URI_WAITQUEUETIMEOUTMS "=2");
   ASSERT_LOG_DUPE (MONGOC_URI_WAITQUEUETIMEOUTMS);
   BSON_ASSERT (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_WAITQUEUETIMEOUTMS, 0) == 2);

   RECREATE_URI (MONGOC_URI_WTIMEOUTMS "=1&" MONGOC_URI_WTIMEOUTMS "=2");
   ASSERT_LOG_DUPE (MONGOC_URI_WTIMEOUTMS);
   BSON_ASSERT (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_WTIMEOUTMS, 0) == 2);
   BSON_ASSERT (mongoc_uri_get_option_as_int64 (uri, MONGOC_URI_WTIMEOUTMS, 0) == 2);

   RECREATE_URI (MONGOC_URI_ZLIBCOMPRESSIONLEVEL "=1&" MONGOC_URI_ZLIBCOMPRESSIONLEVEL "=2");
   ASSERT_LOG_DUPE (MONGOC_URI_ZLIBCOMPRESSIONLEVEL);
   BSON_ASSERT (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_ZLIBCOMPRESSIONLEVEL, 0) == 2);

   mongoc_uri_destroy (uri);
}


/* Tests behavior of int32 and int64 options */
static void
test_mongoc_uri_int_options (void)
{
   mongoc_uri_t *uri;

   capture_logs (true);

   uri = mongoc_uri_new ("mongodb://localhost/");

   /* Set an int64 option as int64 succeeds */
   ASSERT (mongoc_uri_set_option_as_int64 (uri, MONGOC_URI_WTIMEOUTMS, 10));
   ASSERT_CMPINT32 (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_WTIMEOUTMS, 0), ==, 10);
   ASSERT_CMPINT64 (mongoc_uri_get_option_as_int64 (uri, MONGOC_URI_WTIMEOUTMS, 0), ==, 10);

   /* Set an int64 option as int32 succeeds */
   ASSERT (mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_WTIMEOUTMS, 15));
   ASSERT_CMPINT32 (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_WTIMEOUTMS, 0), ==, 15);
   ASSERT_CMPINT64 (mongoc_uri_get_option_as_int64 (uri, MONGOC_URI_WTIMEOUTMS, 0), ==, 15);

   /* Setting an int32 option through _as_int64 succeeds for 32-bit values but
    * emits a warning */
   ASSERT (mongoc_uri_set_option_as_int64 (uri, MONGOC_URI_ZLIBCOMPRESSIONLEVEL, 9));
   ASSERT_CAPTURED_LOG ("option: " MONGOC_URI_ZLIBCOMPRESSIONLEVEL,
                        MONGOC_LOG_LEVEL_WARNING,
                        "Setting value for 32-bit option "
                        "\"zlibcompressionlevel\" through 64-bit method");
   ASSERT_CMPINT32 (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_ZLIBCOMPRESSIONLEVEL, 0), ==, 9);
   ASSERT_CMPINT64 (mongoc_uri_get_option_as_int64 (uri, MONGOC_URI_ZLIBCOMPRESSIONLEVEL, 0), ==, 9);

   clear_captured_logs ();

   ASSERT (!mongoc_uri_set_option_as_int64 (uri, MONGOC_URI_CONNECTTIMEOUTMS, 2147483648LL));
   ASSERT_CAPTURED_LOG ("option: " MONGOC_URI_CONNECTTIMEOUTMS,
                        MONGOC_LOG_LEVEL_WARNING,
                        "Unsupported value for \"connecttimeoutms\": 2147483648,"
                        " \"connecttimeoutms\" is not an int64 option");
   ASSERT_CMPINT32 (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_CONNECTTIMEOUTMS, 0), ==, 0);
   ASSERT_CMPINT64 (mongoc_uri_get_option_as_int64 (uri, MONGOC_URI_CONNECTTIMEOUTMS, 0), ==, 0);

   clear_captured_logs ();

   /* Setting an int32 option as int32 succeeds */
   ASSERT (mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_ZLIBCOMPRESSIONLEVEL, 9));
   ASSERT_CMPINT32 (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_ZLIBCOMPRESSIONLEVEL, 0), ==, 9);
   ASSERT_CMPINT64 (mongoc_uri_get_option_as_int64 (uri, MONGOC_URI_ZLIBCOMPRESSIONLEVEL, 0), ==, 9);

   /* Truncating a 64-bit value when fetching as 32-bit emits a warning */
   ASSERT (mongoc_uri_set_option_as_int64 (uri, MONGOC_URI_WTIMEOUTMS, 2147483648LL));
   ASSERT_CMPINT32 (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_WTIMEOUTMS, 5), ==, 5);
   ASSERT_CAPTURED_LOG ("option: " MONGOC_URI_WTIMEOUTMS " with 64-bit value",
                        MONGOC_LOG_LEVEL_WARNING,
                        "Cannot read 64-bit value for \"wtimeoutms\": 2147483648");
   ASSERT_CMPINT64 (mongoc_uri_get_option_as_int64 (uri, MONGOC_URI_WTIMEOUTMS, 5), ==, 2147483648LL);

   clear_captured_logs ();

   ASSERT (mongoc_uri_set_option_as_int64 (uri, MONGOC_URI_WTIMEOUTMS, -2147483649LL));
   ASSERT_CMPINT32 (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_WTIMEOUTMS, 5), ==, 5);
   ASSERT_CAPTURED_LOG ("option: " MONGOC_URI_WTIMEOUTMS " with 64-bit value",
                        MONGOC_LOG_LEVEL_WARNING,
                        "Cannot read 64-bit value for \"wtimeoutms\": -2147483649");
   ASSERT_CMPINT64 (mongoc_uri_get_option_as_int64 (uri, MONGOC_URI_WTIMEOUTMS, 5), ==, -2147483649LL);

   clear_captured_logs ();

   /* Setting a INT_MAX and INT_MIN values doesn't cause truncation errors */
   ASSERT (mongoc_uri_set_option_as_int64 (uri, MONGOC_URI_WTIMEOUTMS, INT32_MAX));
   ASSERT_CMPINT32 (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_WTIMEOUTMS, 0), ==, INT32_MAX);
   ASSERT_NO_CAPTURED_LOGS ("INT_MAX");
   ASSERT (mongoc_uri_set_option_as_int64 (uri, MONGOC_URI_WTIMEOUTMS, INT32_MIN));
   ASSERT_CMPINT32 (mongoc_uri_get_option_as_int32 (uri, MONGOC_URI_WTIMEOUTMS, 0), ==, INT32_MIN);
   ASSERT_NO_CAPTURED_LOGS ("INT_MIN");

   mongoc_uri_destroy (uri);
}

static void
test_one_tls_option_enables_tls (void)
{
   const char *opts[] = {MONGOC_URI_TLS "=true",
                         MONGOC_URI_TLSCERTIFICATEKEYFILE "=file.pem",
                         MONGOC_URI_TLSCERTIFICATEKEYFILEPASSWORD "=file.pem",
                         MONGOC_URI_TLSCAFILE "=file.pem",
                         MONGOC_URI_TLSALLOWINVALIDCERTIFICATES "=true",
                         MONGOC_URI_TLSALLOWINVALIDHOSTNAMES "=true",
                         MONGOC_URI_TLSINSECURE "=true",
                         MONGOC_URI_SSL "=true",
                         MONGOC_URI_SSLCLIENTCERTIFICATEKEYFILE "=file.pem",
                         MONGOC_URI_SSLCLIENTCERTIFICATEKEYPASSWORD "=file.pem",
                         MONGOC_URI_SSLCERTIFICATEAUTHORITYFILE "=file.pem",
                         MONGOC_URI_SSLALLOWINVALIDCERTIFICATES "=true",
                         MONGOC_URI_SSLALLOWINVALIDHOSTNAMES "=true",
                         MONGOC_URI_TLSDISABLEOCSPENDPOINTCHECK "=true",
                         MONGOC_URI_TLSDISABLECERTIFICATEREVOCATIONCHECK "=true"};
   int i;

   for (i = 0; i < sizeof (opts) / sizeof (opts[0]); i++) {
      mongoc_uri_t *uri;
      bson_error_t error;
      char *uri_string;

      uri_string = bson_strdup_printf ("mongodb://localhost:27017/?%s", opts[i]);
      uri = mongoc_uri_new_with_error (uri_string, &error);
      bson_free (uri_string);
      ASSERT_OR_PRINT (uri, error);
      if (!mongoc_uri_get_tls (uri)) {
         test_error ("unexpected tls not enabled when following option set: %s\n", opts[i]);
      }
      mongoc_uri_destroy (uri);
   }
}

static void
test_casing_options (void)
{
   mongoc_uri_t *uri;
   bson_error_t error;

   uri = mongoc_uri_new ("mongodb://localhost:27017/");
   mongoc_uri_set_option_as_bool (uri, "TLS", true);
   mongoc_uri_parse_options (uri, "ssl=false", false, &error);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "conflicts");

   mongoc_uri_destroy (uri);
}

static void
test_parses_long_ipv6 (void)
{
   // Test parsing long malformed IPv6 literals. This is a regression test for
   // CDRIVER-4816.
   bson_error_t error;

   // Test the largest permitted IPv6 literal.
   {
      // Construct a string of repeating `:`.
      bson_string_t *host = bson_string_new (NULL);
      for (int i = 0; i < BSON_HOST_NAME_MAX - 2; i++) {
         // Max IPv6 literal is two less due to including `[` and `]`.
         bson_string_append (host, ":");
      }

      char *host_and_port = bson_strdup_printf ("[%s]:27017", host->str);
      char *uri_string = bson_strdup_printf ("mongodb://%s", host_and_port);
      mongoc_uri_t *uri = mongoc_uri_new_with_error (uri_string, &error);
      ASSERT_OR_PRINT (uri, error);
      const mongoc_host_list_t *hosts = mongoc_uri_get_hosts (uri);
      ASSERT_CMPSTR (hosts->host, host->str);
      ASSERT_CMPSTR (hosts->host_and_port, host_and_port);
      ASSERT_CMPUINT16 (hosts->port, ==, 27017);
      ASSERT (!hosts->next);

      mongoc_uri_destroy (uri);
      bson_free (uri_string);
      bson_free (host_and_port);
      bson_string_free (host, true /* free_segment */);
   }

   // Test one character more than the largest IPv6 literal.
   {
      // Construct a string of repeating `:`.
      bson_string_t *host = bson_string_new (NULL);
      for (int i = 0; i < BSON_HOST_NAME_MAX - 2 + 1; i++) {
         bson_string_append (host, ":");
      }

      char *host_and_port = bson_strdup_printf ("[%s]:27017", host->str);
      char *uri_string = bson_strdup_printf ("mongodb://%s", host_and_port);
      capture_logs (true);
      mongoc_uri_t *uri = mongoc_uri_new_with_error (uri_string, &error);
      // Expect error parsing IPv6 literal is logged.
      ASSERT_CAPTURED_LOG ("parsing IPv6", MONGOC_LOG_LEVEL_ERROR, "IPv6 literal provided in URI is too long");
      capture_logs (false);

      // Expect a generic parsing error is also returned.
      ASSERT (!uri);
      ASSERT_ERROR_CONTAINS (
         error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Invalid host string in URI");

      mongoc_uri_destroy (uri);
      bson_free (uri_string);
      bson_free (host_and_port);
      bson_string_free (host, true /* free_segment */);
   }
}

void
test_uri_install (TestSuite *suite)
{
   TestSuite_Add (suite, "/Uri/new", test_mongoc_uri_new);
   TestSuite_Add (suite, "/Uri/new_with_error", test_mongoc_uri_new_with_error);
   TestSuite_Add (suite, "/Uri/new_for_host_port", test_mongoc_uri_new_for_host_port);
   TestSuite_Add (suite, "/Uri/compressors", test_mongoc_uri_compressors);
   TestSuite_Add (suite, "/Uri/unescape", test_mongoc_uri_unescape);
   TestSuite_Add (suite, "/Uri/read_prefs", test_mongoc_uri_read_prefs);
   TestSuite_Add (suite, "/Uri/read_concern", test_mongoc_uri_read_concern);
   TestSuite_Add (suite, "/Uri/write_concern", test_mongoc_uri_write_concern);
   TestSuite_Add (suite, "/HostList/from_string", test_mongoc_host_list_from_string);
   TestSuite_Add (suite, "/Uri/auth_mechanism_properties", test_mongoc_uri_authmechanismproperties);
   TestSuite_Add (suite, "/Uri/functions", test_mongoc_uri_functions);
   TestSuite_Add (suite, "/Uri/ssl", test_mongoc_uri_ssl);
   TestSuite_Add (suite, "/Uri/tls", test_mongoc_uri_tls);
   TestSuite_Add (suite, "/Uri/compound_setters", test_mongoc_uri_compound_setters);
   TestSuite_Add (suite, "/Uri/long_hostname", test_mongoc_uri_long_hostname);
   TestSuite_Add (suite, "/Uri/local_threshold_ms", test_mongoc_uri_local_threshold_ms);
   TestSuite_Add (suite, "/Uri/srv", test_mongoc_uri_srv);
   TestSuite_Add (suite, "/Uri/dns_options", test_mongoc_uri_dns_options);
   TestSuite_Add (suite, "/Uri/utf8", test_mongoc_uri_utf8);
   TestSuite_Add (suite, "/Uri/duplicates", test_mongoc_uri_duplicates);
   TestSuite_Add (suite, "/Uri/int_options", test_mongoc_uri_int_options);
   TestSuite_Add (suite, "/Uri/one_tls_option_enables_tls", test_one_tls_option_enables_tls);
   TestSuite_Add (suite, "/Uri/options_casing", test_casing_options);
   TestSuite_Add (suite, "/Uri/parses_long_ipv6", test_parses_long_ipv6);
}
