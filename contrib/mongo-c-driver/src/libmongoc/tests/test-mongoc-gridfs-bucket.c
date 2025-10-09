#include "mock_server/mock-server.h"
#include "mock_server/future.h"
#include "mock_server/future-functions.h"
#include "mongoc/mongoc.h"
#include "mongoc/mongoc-gridfs-bucket-private.h"
#include "json-test.h"
#include "TestSuite.h"
#include "test-conveniences.h"
#include "test-libmongoc.h"

void
test_create_bucket (void)
{
   /* Tests creating a bucket with all opts set */
   mongoc_gridfs_bucket_t *gridfs;
   mongoc_read_prefs_t *read_prefs;
   mongoc_write_concern_t *write_concern;
   mongoc_read_concern_t *read_concern;
   mongoc_database_t *db;
   mongoc_client_t *client;
   bson_t *opts;
   char *dbname;

   client = test_framework_new_default_client ();
   ASSERT (client);

   dbname = gen_collection_name ("test");

   db = mongoc_client_get_database (client, dbname);

   bson_free (dbname);

   ASSERT (db);

   opts = bson_new ();

   /* write concern */
   write_concern = mongoc_write_concern_new ();
   mongoc_write_concern_set_w (write_concern, 1);
   mongoc_write_concern_append (write_concern, opts);

   /* read concern */
   read_concern = mongoc_read_concern_new ();
   mongoc_read_concern_set_level (read_concern, MONGOC_READ_CONCERN_LEVEL_LOCAL);
   mongoc_read_concern_append (read_concern, opts);

   /* other opts */
   BSON_APPEND_UTF8 (opts, "bucketName", "test-gridfs");
   BSON_APPEND_INT32 (opts, "chunkSizeBytes", 10);

   read_prefs = mongoc_read_prefs_new (MONGOC_READ_PRIMARY);

   gridfs = mongoc_gridfs_bucket_new (db, opts, read_prefs, NULL);

   ASSERT (gridfs);

   mongoc_gridfs_bucket_destroy (gridfs);
   bson_destroy (opts);
   mongoc_write_concern_destroy (write_concern);
   mongoc_read_concern_destroy (read_concern);
   mongoc_read_prefs_destroy (read_prefs);
   mongoc_database_destroy (db);
   mongoc_client_destroy (client);
}

static int
_get_index_count (mongoc_collection_t *collection)
{
   mongoc_cursor_t *cursor;
   bson_error_t error = {0};
   int n;
   const bson_t *info;

   cursor = mongoc_collection_find_indexes (collection, &error);
   ASSERT_OR_PRINT (0 == error.code, error);

   n = 0;

   while (mongoc_cursor_next (cursor, &info)) {
      n++;
   }

   mongoc_cursor_destroy (cursor);

   return n;
}

/* Util that downloads a file content into the given buffer. Returns num bytes
 * read. */
static size_t
_download_file_into_buf (mongoc_gridfs_bucket_t *bucket, const bson_value_t *file_id, char *buf, size_t len)
{
   bson_error_t error;
   size_t nread;
   mongoc_stream_t *down = mongoc_gridfs_bucket_open_download_stream (bucket, file_id, &error);
   ASSERT_OR_PRINT (down, error);
   nread = mongoc_stream_read (down, buf, len, 1 /* min read */, 0 /* No timeout */);
   mongoc_stream_destroy (down);
   ASSERT (nread > 0);
   return nread;
}

/* Util for uploading a file with the given string as content */
static void
_upload_file_from_str (
   mongoc_gridfs_bucket_t *bucket, const char *filename, const char *content, const bson_t *opts, bson_value_t *file_id)
{
   bson_error_t error;
   mongoc_stream_t *const up = mongoc_gridfs_bucket_open_upload_stream (bucket, filename, opts, file_id, &error);
   ASSERT_OR_PRINT (up, error);
   const size_t nwritten = mongoc_stream_write (up, (void *) content, strlen (content), 0);
   ASSERT_CMPSIZE_T (nwritten, ==, strlen (content));
   mongoc_stream_destroy (up);
}

void
_test_upload_and_download (bson_t *create_index_cmd)
{
   mongoc_gridfs_bucket_t *gridfs;
   bson_value_t file_id;
   mongoc_database_t *db;
   mongoc_client_t *client;
   bson_t *opts;
   /* big enough to hold all of str */
   char buf[100] = {0};
   char *str;
   char *dbname;

   str = "This is a test sentence with multiple chunks.";

   client = test_framework_new_default_client ();

   ASSERT (client);

   dbname = gen_collection_name ("test");

   db = mongoc_client_get_database (client, dbname);

   bson_free (dbname);

   ASSERT (db);

   if (create_index_cmd) {
      bson_error_t error;

      ASSERT_OR_PRINT (mongoc_database_write_command_with_opts (db, create_index_cmd, NULL, NULL, &error), error);
   }

   opts = bson_new ();
   BSON_APPEND_INT32 (opts, "chunkSizeBytes", 10);

   gridfs = mongoc_gridfs_bucket_new (db, opts, NULL, NULL);

   _upload_file_from_str (gridfs, "my-file", str, opts, &file_id);

   /* download str into the buffer from gridfs. */
   _download_file_into_buf (gridfs, &file_id, buf, sizeof buf);

   /* compare. */
   ASSERT (strcmp (buf, str) == 0);

   ASSERT_CMPINT (_get_index_count (gridfs->files), ==, 2);
   ASSERT_CMPINT (_get_index_count (gridfs->chunks), ==, 2);

   bson_destroy (opts);
   mongoc_gridfs_bucket_destroy (gridfs);
   mongoc_database_destroy (db);
   mongoc_client_destroy (client);
}

void
test_upload_and_download (void)
{
   _test_upload_and_download (NULL);

   /* Test files index with float and same options */
   _test_upload_and_download (tmp_bson ("{'createIndexes': '%s',"
                                        " 'indexes': [{'key': {'filename': 1.0, 'uploadDate': 1}, "
                                        "'name': 'filename_1_uploadDate_1'}]}",
                                        "fs.files"));

   /* Files index with float and different options */
   _test_upload_and_download (tmp_bson ("{'createIndexes': '%s',"
                                        " 'indexes': [{'key': {'filename': 1.0, 'uploadDate': 1}, "
                                        "'name': 'different_name'}]}",
                                        "fs.files"));

   /* Chunks index with float and same options */
   _test_upload_and_download (tmp_bson ("{'createIndexes': '%s',"
                                        " 'indexes': [{'key': {'files_id': 1.0, 'n': 1}, 'name': "
                                        "'files_id_1_n_1', 'unique': true}]}",
                                        "fs.chunks"));

   /* Chunks index with float and different options */
   _test_upload_and_download (tmp_bson ("{'createIndexes': '%s',"
                                        " 'indexes': [{'key': {'files_id': 1.0, 'n': 1}, 'name': "
                                        "'different_name', 'unique': true}]}",
                                        "fs.chunks"));
}

bool
hex_to_bytes (const char *hex_str, size_t *size /* OUT */, uint8_t **bytes /* OUT */)
{
   size_t len;
   uint8_t *result;
   size_t i;

   len = strlen (hex_str);

   ASSERT (bytes);
   ASSERT (len % 2 == 0);

   if (len == 0) {
      return false;
   }

   result = (uint8_t *) bson_malloc0 (len / 2);

   for (i = 0; i < len; i += 2) {
      sscanf (hex_str + i, "%2hhx", &result[i / 2]);
   }

   *bytes = result;

   if (size) {
      *size = (len / 2);
   }

   return true;
}

bson_t *
convert_hex_to_binary (bson_t *doc)
{
   /* PLAN: Recurse to all sub documents and call this function, otherwise, just
    * append */
   bson_iter_t iter;
   bson_iter_t inner;
   bson_t *result;
   const char *key;
   bson_t *sub_doc;
   bson_t *sub_doc_result;
   const bson_value_t *value;

   bson_t *hex_doc;
   uint8_t *hex_bytes;
   size_t hex_len;
   const char *str;
   bool r;

   result = bson_new ();

   bson_iter_init (&iter, doc);

   while (bson_iter_next (&iter)) {
      key = bson_iter_key (&iter);
      value = bson_iter_value (&iter);

      if (strcmp (key, "data") == 0) {
         hex_doc = bson_new_from_data (bson_iter_value (&iter)->value.v_doc.data,
                                       bson_iter_value (&iter)->value.v_doc.data_len);

         ASSERT (bson_iter_init_find (&inner, hex_doc, "$hex"));

         str = bson_iter_utf8 (&inner, NULL);
         r = hex_to_bytes (str, &hex_len, &hex_bytes);

         if (r) {
            BSON_APPEND_BINARY (result, key, BSON_SUBTYPE_BINARY, hex_bytes, (uint32_t) hex_len);
            bson_free (hex_bytes);
         } else {
            BSON_APPEND_BINARY (result, key, BSON_SUBTYPE_BINARY, (const uint8_t *) str, 0);
         }

         bson_destroy (hex_doc);

      } else if (value->value_type == BSON_TYPE_DOCUMENT || value->value_type == BSON_TYPE_ARRAY) {
         sub_doc = bson_new_from_data (bson_iter_value (&iter)->value.v_doc.data,
                                       bson_iter_value (&iter)->value.v_doc.data_len);

         sub_doc_result = convert_hex_to_binary (sub_doc);
         bson_destroy (sub_doc);
         if (value->value_type == BSON_TYPE_DOCUMENT) {
            BSON_APPEND_DOCUMENT (result, key, sub_doc_result);
         } else {
            BSON_APPEND_ARRAY (result, key, sub_doc_result);
         }
         bson_destroy (sub_doc_result);
      } else {
         BSON_APPEND_VALUE (result, key, value);
      }
   }

   return result;
}

/*
 * Initializes the proper GridFS collections with the provided data
 */
void
setup_gridfs_collections (mongoc_database_t *db, bson_t *data)
{
   mongoc_collection_t *files;
   mongoc_collection_t *chunks;
   mongoc_collection_t *expected_files;
   mongoc_collection_t *expected_chunks;
   bson_iter_t iter;
   bson_iter_t inner;
   bson_t *selector;
   bool r;

   files = mongoc_database_get_collection (db, "fs.files");
   chunks = mongoc_database_get_collection (db, "fs.chunks");
   expected_files = mongoc_database_get_collection (db, "expected.files");
   expected_chunks = mongoc_database_get_collection (db, "expected.chunks");

   selector = bson_new ();

   mongoc_collection_delete_many (files, selector, NULL, NULL, NULL);
   mongoc_collection_delete_many (chunks, selector, NULL, NULL, NULL);
   mongoc_collection_delete_many (expected_files, selector, NULL, NULL, NULL);
   mongoc_collection_delete_many (expected_chunks, selector, NULL, NULL, NULL);

   bson_destroy (selector);

   if (bson_iter_init_find (&iter, data, "files")) {
      bson_t *docs =
         bson_new_from_data (bson_iter_value (&iter)->value.v_doc.data, bson_iter_value (&iter)->value.v_doc.data_len);

      bson_iter_init (&inner, docs);
      while (bson_iter_next (&inner)) {
         bson_t *doc = bson_new_from_data (bson_iter_value (&inner)->value.v_doc.data,
                                           bson_iter_value (&inner)->value.v_doc.data_len);
         r = mongoc_collection_insert_one (files, doc, NULL, NULL, NULL);
         ASSERT (r);
         r = mongoc_collection_insert_one (expected_files, doc, NULL, NULL, NULL);
         ASSERT (r);
         bson_destroy (doc);
      }

      bson_destroy (docs);
   }

   if (bson_iter_init_find (&iter, data, "chunks")) {
      bson_t *docs =
         bson_new_from_data (bson_iter_value (&iter)->value.v_doc.data, bson_iter_value (&iter)->value.v_doc.data_len);

      bson_iter_init (&inner, docs);
      while (bson_iter_next (&inner)) {
         bson_t *doc = bson_new_from_data (bson_iter_value (&inner)->value.v_doc.data,
                                           bson_iter_value (&inner)->value.v_doc.data_len);
         bson_t *chunk = convert_hex_to_binary (doc);
         r = mongoc_collection_insert_one (chunks, chunk, NULL, NULL, NULL);
         ASSERT (r);
         r = mongoc_collection_insert_one (expected_chunks, chunk, NULL, NULL, NULL);
         ASSERT (r);
         bson_destroy (doc);
         bson_destroy (chunk);
      }

      bson_destroy (docs);
   }

   mongoc_collection_destroy (files);
   mongoc_collection_destroy (chunks);
   mongoc_collection_destroy (expected_files);
   mongoc_collection_destroy (expected_chunks);
}

void
gridfs_spec_run_commands (mongoc_database_t *db, bson_t *commands)
{
   bson_iter_t iter;
   bson_t *data;
   bson_t *command;
   bson_t *hex_command;
   bool r;

   ASSERT (bson_iter_init_find (&iter, commands, "data"));
   data = bson_new_from_data (bson_iter_value (&iter)->value.v_doc.data, bson_iter_value (&iter)->value.v_doc.data_len);

   bson_iter_init (&iter, data);

   while (bson_iter_next (&iter)) {
      command =
         bson_new_from_data (bson_iter_value (&iter)->value.v_doc.data, bson_iter_value (&iter)->value.v_doc.data_len);

      hex_command = convert_hex_to_binary (command);

      r = mongoc_database_command_simple (db, hex_command, NULL, NULL, NULL);
      ASSERT (r);
      bson_destroy (command);
      bson_destroy (hex_command);
   }
   bson_destroy (data);
}

bson_t *
gridfs_replace_result (bson_t *doc, bson_value_t *replacement)
{
   bson_iter_t iter;
   bson_t *result;
   const char *key;
   bson_t *sub_doc;
   bson_t *sub_doc_result;
   const bson_value_t *value;

   const char *str;

   result = bson_new ();

   bson_iter_init (&iter, doc);

   while (bson_iter_next (&iter)) {
      key = bson_iter_key (&iter);
      value = bson_iter_value (&iter);

      if (value->value_type == BSON_TYPE_UTF8) {
         str = bson_iter_utf8 (&iter, NULL);

         if (strcmp (str, "*result") == 0) {
            BSON_APPEND_VALUE (result, key, replacement);
         } else if (strcmp (str, "*actual") == 0) {
            /* Skip adding this */
         } else {
            BSON_APPEND_VALUE (result, key, value);
         }

      } else if (value->value_type == BSON_TYPE_DOCUMENT || value->value_type == BSON_TYPE_ARRAY) {
         sub_doc = bson_new_from_data (bson_iter_value (&iter)->value.v_doc.data,
                                       bson_iter_value (&iter)->value.v_doc.data_len);

         sub_doc_result = gridfs_replace_result (sub_doc, replacement);
         bson_destroy (sub_doc);
         if (value->value_type == BSON_TYPE_DOCUMENT) {
            BSON_APPEND_DOCUMENT (result, key, sub_doc_result);
         } else {
            BSON_APPEND_ARRAY (result, key, sub_doc_result);
         }
         bson_destroy (sub_doc_result);
      } else {
         BSON_APPEND_VALUE (result, key, value);
      }
   }

   return result;
}

void
gridfs_compare_collections (mongoc_database_t *db)
{
   mongoc_collection_t *files;
   mongoc_collection_t *chunks;
   mongoc_collection_t *expected_files;
   mongoc_collection_t *expected_chunks;
   mongoc_cursor_t *expected_cursor;
   mongoc_cursor_t *actual_cursor;
   const bson_t *expected_doc;
   const bson_t *actual_doc;
   bson_iter_t expected_iter;
   bson_iter_t actual_iter;
   const uint8_t *expected_binary;
   const uint8_t *actual_binary;
   uint32_t expected_len;
   uint32_t actual_len;
   bson_t filter;

   files = mongoc_database_get_collection (db, "fs.files");
   chunks = mongoc_database_get_collection (db, "fs.chunks");
   expected_files = mongoc_database_get_collection (db, "expected.files");
   expected_chunks = mongoc_database_get_collection (db, "expected.chunks");

   bson_init (&filter);

   /* Compare files collections */
   actual_cursor = mongoc_collection_find_with_opts (files, &filter, NULL, NULL);
   expected_cursor = mongoc_collection_find_with_opts (expected_files, &filter, NULL, NULL);

   while (mongoc_cursor_next (expected_cursor, &expected_doc)) {
      ASSERT (mongoc_cursor_next (actual_cursor, &actual_doc));

      ASSERT (bson_iter_init_find (&actual_iter, actual_doc, "_id"));
      ASSERT (bson_iter_init_find (&expected_iter, expected_doc, "_id"));
      ASSERT (bson_oid_compare (bson_iter_oid (&actual_iter), bson_iter_oid (&expected_iter)) == 0);

      ASSERT (bson_iter_init_find (&actual_iter, actual_doc, "length"));
      ASSERT (bson_iter_init_find (&expected_iter, expected_doc, "length"));
      ASSERT (bson_iter_as_int64 (&actual_iter) == bson_iter_as_int64 (&expected_iter));

      ASSERT (bson_iter_init_find (&actual_iter, actual_doc, "chunkSize"));
      ASSERT (bson_iter_init_find (&expected_iter, expected_doc, "chunkSize"));
      ASSERT (bson_iter_int32 (&actual_iter) == bson_iter_int32 (&expected_iter));

      ASSERT (bson_iter_init_find (&actual_iter, actual_doc, "filename"));
      ASSERT (bson_iter_init_find (&expected_iter, expected_doc, "filename"));
      ASSERT (strcmp (bson_iter_utf8 (&actual_iter, NULL), bson_iter_utf8 (&expected_iter, NULL)) == 0);
   }

   /* Make sure the actual doesn't have extra docs */
   ASSERT (!mongoc_cursor_next (actual_cursor, &actual_doc));

   mongoc_cursor_destroy (actual_cursor);
   mongoc_cursor_destroy (expected_cursor);


   /* Compare chunks collections */
   actual_cursor = mongoc_collection_find_with_opts (chunks, &filter, NULL, NULL);
   expected_cursor = mongoc_collection_find_with_opts (expected_chunks, &filter, NULL, NULL);


   while (mongoc_cursor_next (expected_cursor, &expected_doc)) {
      ASSERT (mongoc_cursor_next (actual_cursor, &actual_doc));

      ASSERT (bson_iter_init_find (&actual_iter, actual_doc, "files_id"));
      ASSERT (bson_iter_init_find (&expected_iter, expected_doc, "files_id"));
      ASSERT (bson_oid_compare (bson_iter_oid (&actual_iter), bson_iter_oid (&expected_iter)) == 0);

      ASSERT (bson_iter_init_find (&actual_iter, actual_doc, "n"));
      ASSERT (bson_iter_init_find (&expected_iter, expected_doc, "n"));
      ASSERT (bson_iter_int32 (&actual_iter) == bson_iter_int32 (&expected_iter));

      ASSERT (bson_iter_init_find (&actual_iter, actual_doc, "data"));
      ASSERT (bson_iter_init_find (&expected_iter, expected_doc, "data"));
      bson_iter_binary (&actual_iter, NULL, &actual_len, &actual_binary);
      bson_iter_binary (&expected_iter, NULL, &expected_len, &expected_binary);
      ASSERT (actual_len == expected_len);
      ASSERT (memcmp (actual_binary, expected_binary, actual_len) == 0);
   }

   /* Make sure the actual doesn't have extra docs */
   ASSERT (!mongoc_cursor_next (actual_cursor, &actual_doc));

   bson_destroy (&filter);
   mongoc_cursor_destroy (actual_cursor);
   mongoc_cursor_destroy (expected_cursor);

   mongoc_collection_destroy (files);
   mongoc_collection_destroy (chunks);
   mongoc_collection_destroy (expected_files);
   mongoc_collection_destroy (expected_chunks);
}

void
gridfs_spec_delete_operation (mongoc_database_t *db, mongoc_gridfs_bucket_t *bucket, bson_t *act, bson_t *assert)
{
   bson_iter_t iter;
   bson_t *arguments;
   const bson_value_t *value;
   bool r;

   ASSERT (bson_iter_init_find (&iter, act, "arguments"));
   arguments =
      bson_new_from_data (bson_iter_value (&iter)->value.v_doc.data, bson_iter_value (&iter)->value.v_doc.data_len);

   ASSERT (bson_iter_init_find (&iter, arguments, "id"));
   value = bson_iter_value (&iter);

   r = mongoc_gridfs_bucket_delete_by_id (bucket, value, NULL);
   ASSERT (r != bson_iter_init_find (&iter, assert, "error"));

   if (bson_iter_init_find (&iter, assert, "data")) {
      gridfs_spec_run_commands (db, assert);
   }

   /* compare collections! */
   gridfs_compare_collections (db);

   bson_destroy (arguments);
}

void
gridfs_spec_download_operation (mongoc_database_t *db, mongoc_gridfs_bucket_t *bucket, bson_t *act, bson_t *assert)
{
   bson_iter_t iter;
   bson_t *arguments;
   bson_t *hex_doc;
   const bson_value_t *value;
   mongoc_stream_t *stream;
   char buf[100] = {0};
   uint8_t *hex_bytes;
   size_t hex_len;
   ssize_t ret;
   const char *str;
   const char *expected_error;
   bson_error_t error;
   bool r;

   BSON_UNUSED (db);

   ASSERT (bson_iter_init_find (&iter, act, "arguments"));
   arguments =
      bson_new_from_data (bson_iter_value (&iter)->value.v_doc.data, bson_iter_value (&iter)->value.v_doc.data_len);

   if (bson_iter_init_find (&iter, assert, "error")) {
      expected_error = bson_iter_utf8 (&iter, NULL);
   } else {
      expected_error = "";
   }

   ASSERT (bson_iter_init_find (&iter, arguments, "id"));
   value = bson_iter_value (&iter);

   stream = mongoc_gridfs_bucket_open_download_stream (bucket, value, &error);

   if (strcmp (expected_error, "FileNotFound") == 0) {
      ASSERT (!stream);
      ASSERT (error.code);
      bson_destroy (arguments);
      return;
   }

   ret = mongoc_stream_read (stream, buf, 100, 0, 0);

   if (strcmp (expected_error, "ChunkIsMissing") == 0 || strcmp (expected_error, "ChunkIsWrongSize") == 0) {
      ASSERT (ret < 0);
      r = mongoc_gridfs_bucket_stream_error (stream, &error);
      ASSERT (r);
      ASSERT (error.code);
      bson_destroy (arguments);
      mongoc_stream_destroy (stream);
      return;
   }

   mongoc_stream_close (stream);

   ASSERT (bson_iter_init_find (&iter, assert, "result"));
   hex_doc =
      bson_new_from_data (bson_iter_value (&iter)->value.v_doc.data, bson_iter_value (&iter)->value.v_doc.data_len);

   ASSERT (bson_iter_init_find (&iter, hex_doc, "$hex"));
   str = bson_iter_utf8 (&iter, NULL);

   r = hex_to_bytes (str, &hex_len, &hex_bytes);
   if (r) {
      ASSERT (ret == hex_len);
      ASSERT (memcmp (buf, hex_bytes, hex_len) == 0);
      bson_free (hex_bytes);
   } else {
      ASSERT (ret == 0);
   }

   /* Make sure we don't need to run any commands */
   ASSERT (!bson_iter_init_find (&iter, assert, "data"));

   bson_destroy (hex_doc);
   bson_destroy (arguments);
   mongoc_stream_destroy (stream);
}

void
gridfs_spec_download_by_name_operation (mongoc_database_t *db,
                                        mongoc_gridfs_bucket_t *bucket,
                                        bson_t *act,
                                        bson_t *assert)
{
   BSON_UNUSED (db);
   BSON_UNUSED (bucket);
   BSON_UNUSED (act);
   BSON_UNUSED (assert);

   /* The download_by_name functionality is part of the Advanced API for GridFS
    * and the C Driver hasn't implemented the Advanced API yet. This is a
    * placeholder to be used when the download_by_name is implemented. */
}

void
gridfs_spec_upload_operation (mongoc_database_t *db, mongoc_gridfs_bucket_t *bucket, bson_t *act, bson_t *assert)
{
   bson_iter_t iter;
   bson_t *arguments;
   const char *filename;
   mongoc_stream_t *stream;
   uint8_t *hex_bytes;
   bson_t *hex_doc;
   bson_t *options;
   ssize_t bytes_written;
   const char *str;
   bson_value_t file_id;
   bson_t *assert_modified;
   size_t hex_len;
   bool r;

   ASSERT (bson_iter_init_find (&iter, act, "arguments"));
   arguments =
      bson_new_from_data (bson_iter_value (&iter)->value.v_doc.data, bson_iter_value (&iter)->value.v_doc.data_len);

   ASSERT (bson_iter_init_find (&iter, arguments, "filename"));
   filename = bson_iter_utf8 (&iter, NULL);

   ASSERT (bson_iter_init_find (&iter, arguments, "source"));
   hex_doc =
      bson_new_from_data (bson_iter_value (&iter)->value.v_doc.data, bson_iter_value (&iter)->value.v_doc.data_len);

   ASSERT (bson_iter_init_find (&iter, hex_doc, "$hex"));
   str = bson_iter_utf8 (&iter, NULL);

   r = hex_to_bytes (str, &hex_len, &hex_bytes);
   if (!r) {
      hex_len = 0;
      hex_bytes = bson_malloc0 (1);
   }

   ASSERT (bson_iter_init_find (&iter, arguments, "options"));
   options =
      bson_new_from_data (bson_iter_value (&iter)->value.v_doc.data, bson_iter_value (&iter)->value.v_doc.data_len);

   stream = mongoc_gridfs_bucket_open_upload_stream (bucket, filename, options, &file_id, NULL);

   ASSERT (stream);

   bytes_written = mongoc_stream_write (stream, hex_bytes, hex_len, 0);
   ASSERT (bytes_written == hex_len);
   bson_free (hex_bytes);

   mongoc_stream_close (stream);
   mongoc_stream_destroy (stream);

   assert_modified = gridfs_replace_result (assert, &file_id);
   gridfs_spec_run_commands (db, assert_modified);
   gridfs_compare_collections (db);

   bson_destroy (assert_modified);
   bson_destroy (options);
   bson_destroy (hex_doc);
   bson_destroy (arguments);
}
void
run_gridfs_spec_test (mongoc_database_t *db, mongoc_gridfs_bucket_t *bucket, bson_t *test)
{
   bson_iter_t iter;
   bson_iter_t inner;
   bson_t *act;
   bson_t *assert;
   bson_t *arrange;
   const char *operation;

   ASSERT (bson_iter_init_find (&iter, test, "act"));
   act = bson_new_from_data (bson_iter_value (&iter)->value.v_doc.data, bson_iter_value (&iter)->value.v_doc.data_len);

   ASSERT (bson_iter_init_find (&iter, test, "assert"));
   assert =
      bson_new_from_data (bson_iter_value (&iter)->value.v_doc.data, bson_iter_value (&iter)->value.v_doc.data_len);

   if (bson_iter_init_find (&iter, test, "arrange")) {
      arrange =
         bson_new_from_data (bson_iter_value (&iter)->value.v_doc.data, bson_iter_value (&iter)->value.v_doc.data_len);
      gridfs_spec_run_commands (db, arrange);
      bson_destroy (arrange);
   }

   ASSERT (bson_iter_init_find (&inner, act, "operation"));
   operation = bson_iter_utf8 (&inner, NULL);

   if (strcmp (operation, "delete") == 0) {
      gridfs_spec_delete_operation (db, bucket, act, assert);
   } else if (strcmp (operation, "download") == 0) {
      gridfs_spec_download_operation (db, bucket, act, assert);
   } else if (strcmp (operation, "download_by_name") == 0) {
      gridfs_spec_download_by_name_operation (db, bucket, act, assert);
   } else if (strcmp (operation, "upload") == 0) {
      gridfs_spec_upload_operation (db, bucket, act, assert);
   } else {
      /* Shouldn't happen. */
      ASSERT (false);
   }

   bson_destroy (act);
   bson_destroy (assert);
}

static void
test_gridfs_cb (bson_t *scenario)
{
   mongoc_gridfs_bucket_t *gridfs;
   mongoc_database_t *db;
   mongoc_client_t *client;
   bson_iter_t iter;
   bson_iter_t inner;
   char *dbname;
   bson_t *data;
   bson_t *tests;
   bson_t *test;

   /* Make a gridfs on generated db */
   dbname = gen_collection_name ("test");
   client = test_framework_new_default_client ();
   db = mongoc_client_get_database (client, dbname);
   gridfs = mongoc_gridfs_bucket_new (db, NULL, NULL, NULL);

   /* Insert the data */
   if (bson_iter_init_find (&iter, scenario, "data")) {
      data =
         bson_new_from_data (bson_iter_value (&iter)->value.v_doc.data, bson_iter_value (&iter)->value.v_doc.data_len);
      setup_gridfs_collections (db, data);
      bson_destroy (data);
   }

   /* Run the tests */
   if (bson_iter_init_find (&iter, scenario, "tests")) {
      tests =
         bson_new_from_data (bson_iter_value (&iter)->value.v_doc.data, bson_iter_value (&iter)->value.v_doc.data_len);

      bson_iter_init (&inner, tests);
      while (bson_iter_next (&inner)) {
         test = bson_new_from_data (bson_iter_value (&inner)->value.v_doc.data,
                                    bson_iter_value (&inner)->value.v_doc.data_len);
         run_gridfs_spec_test (db, gridfs, test);
         bson_destroy (test);
      }

      bson_destroy (tests);
   }

   bson_free (dbname);
   mongoc_gridfs_bucket_destroy (gridfs);
   mongoc_database_destroy (db);
   mongoc_client_destroy (client);
}

static void
test_all_spec_tests (TestSuite *suite)
{
   install_json_test_suite (suite, JSON_DIR, "gridfs", &test_gridfs_cb);
}

static void
test_upload_error (void *ctx)
{
   mongoc_client_t *client;
   mongoc_uri_t *uri;
   mongoc_database_t *db;
   mongoc_gridfs_bucket_t *gridfs;
   mongoc_stream_t *source;
   bson_error_t error = {0};
   char *const dbname = gen_collection_name ("test_upload_error");
   bool r;

   BSON_UNUSED (ctx);

   client = test_framework_new_default_client ();
   db = mongoc_client_get_database (client, dbname);
   gridfs = mongoc_gridfs_bucket_new (db, NULL, NULL, NULL);
   source = mongoc_stream_file_new_for_path (BSON_BINARY_DIR "/test1.bson", O_RDONLY, 0);
   BSON_ASSERT (source);
   r = mongoc_gridfs_bucket_upload_from_stream (gridfs, "test1", source, NULL /* opts */, NULL /* file id */, &error);
   ASSERT_OR_PRINT (r, error);

   /* create a read-only user */
   (void) mongoc_database_remove_user (db, "fake_user", NULL);
   r = mongoc_database_add_user (db, "fake_user", "password", tmp_bson ("{'0': 'read'}"), NULL, &error);
   ASSERT_OR_PRINT (r, error);

   mongoc_stream_close (source);
   mongoc_stream_destroy (source);
   mongoc_gridfs_bucket_destroy (gridfs);
   mongoc_database_destroy (db);
   mongoc_client_destroy (client);

   /* initialize gridfs with a root user. */
   uri = test_framework_get_uri ();
   mongoc_uri_set_username (uri, "fake_user");
   mongoc_uri_set_password (uri, "password");
   client = test_framework_client_new_from_uri (uri, NULL);
   test_framework_set_ssl_opts (client);
   mongoc_uri_destroy (uri);

   source = mongoc_stream_file_new_for_path (BSON_BINARY_DIR "/test1.bson", O_RDONLY, 0);
   BSON_ASSERT (source);
   db = mongoc_client_get_database (client, dbname);
   gridfs = mongoc_gridfs_bucket_new (db, NULL, NULL, NULL);
   mongoc_gridfs_bucket_upload_from_stream (gridfs, "test1", source, NULL /* opts */, NULL /* file id */, &error);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_CLIENT, MONGOC_ERROR_CLIENT_AUTHENTICATE, "");

   mongoc_stream_close (source);
   mongoc_stream_destroy (source);
   mongoc_gridfs_bucket_destroy (gridfs);
   bson_free (dbname);
   mongoc_database_destroy (db);
   mongoc_client_destroy (client);
}

static void
test_find_w_session (void *ctx)
{
   mongoc_client_t *client;
   mongoc_database_t *db;
   mongoc_gridfs_bucket_t *gridfs;
   mongoc_cursor_t *cursor;
   bson_error_t error = {0};
   bson_t opts;
   mongoc_client_session_t *session;
   char *dbname = gen_collection_name ("test_find_w_session");
   bool r;

   BSON_UNUSED (ctx);

   client = test_framework_new_default_client ();
   db = mongoc_client_get_database (client, dbname);
   gridfs = mongoc_gridfs_bucket_new (db, NULL, NULL, NULL);
   session = mongoc_client_start_session (client, NULL, &error);
   ASSERT_OR_PRINT (session, error);
   bson_init (&opts);
   r = mongoc_client_session_append (session, &opts, &error);
   ASSERT_OR_PRINT (r, error);
   cursor = mongoc_gridfs_bucket_find (gridfs, tmp_bson ("{}"), &opts);
   BSON_ASSERT (mongoc_cursor_error (cursor, &error));
   ASSERT_ERROR_CONTAINS (
      error, MONGOC_ERROR_CURSOR, MONGOC_ERROR_CURSOR_INVALID_CURSOR, "Cannot pass sessionId as an option");
   bson_destroy (&opts);
   mongoc_cursor_destroy (cursor);
   mongoc_gridfs_bucket_destroy (gridfs);
   mongoc_client_session_destroy (session);
   mongoc_database_destroy (db);
   bson_free (dbname);
   mongoc_client_destroy (client);
}

static void
test_find (void *ctx)
{
   mongoc_client_t *const client = test_framework_new_default_client ();
   char *const dbname = gen_collection_name ("test_find");
   mongoc_database_t *const db = mongoc_client_get_database (client, dbname);
   mongoc_gridfs_bucket_t *const gridfs = mongoc_gridfs_bucket_new (db, NULL, NULL, NULL);
   mongoc_cursor_t *cursor;
   bson_error_t error = {0};
   bson_t const *found;
   bson_iter_t iter;
   char buffer[256] = {0};
   bool ok;
   bson_value_t const *found_id;
   const bson_t *const find_opts = tmp_bson ("{'limit': 1, 'skip': 2, 'sort': {'metadata.testOrder': -1}}");

   BSON_UNUSED (ctx);

   _upload_file_from_str (gridfs, "file1", "First file", tmp_bson ("{'metadata': {'testOrder': 1}}"), NULL);
   _upload_file_from_str (gridfs, "file2", "Second file", tmp_bson ("{'metadata': {'testOrder': 2}}"), NULL);
   _upload_file_from_str (gridfs, "file3", "Third file", tmp_bson ("{'metadata': {'testOrder': 3}}"), NULL);
   _upload_file_from_str (gridfs, "file4", "Fourth file", tmp_bson ("{'metadata': {'testOrder': 4}}"), NULL);

   cursor = mongoc_gridfs_bucket_find (gridfs, tmp_bson ("{}"), find_opts);
   ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error), error);

   ok = mongoc_cursor_next (cursor, &found);
   ASSERT (ok && "No files returned");
   ok = bson_iter_init_find (&iter, found, "_id");
   ASSERT (ok && "Document has no '_id' ??");
   found_id = bson_iter_value (&iter);

   _download_file_into_buf (gridfs, found_id, buffer, sizeof buffer);
   ASSERT_CMPSTR (buffer, "Second file");

   ok = mongoc_cursor_next (cursor, &found);
   ASSERT (!(ok && "More than one file returned"));

   mongoc_cursor_destroy (cursor);
   mongoc_gridfs_bucket_destroy (gridfs);
   bson_free (dbname);
   mongoc_database_destroy (db);
   mongoc_client_destroy (client);
}

void
test_gridfs_bucket_opts (void)
{
   mongoc_client_t *client;
   mongoc_database_t *db;
   mongoc_gridfs_bucket_t *gridfs;
   bson_error_t error;
   mongoc_read_concern_t *rc;
   mongoc_write_concern_t *wc;
   bson_t *opts;
   char *bucket_name;

   client = test_framework_new_default_client ();
   db = mongoc_client_get_database (client, "test");

   /* check defaults. */
   gridfs = mongoc_gridfs_bucket_new (db, NULL, NULL, &error);
   ASSERT_OR_PRINT (gridfs, error);
   ASSERT_CMPSTR (gridfs->bucket_name, "fs");
   ASSERT_CMPINT32 (gridfs->chunk_size, ==, 255 * 1024);
   BSON_ASSERT (!mongoc_read_concern_get_level (mongoc_collection_get_read_concern (gridfs->chunks)));
   BSON_ASSERT (!mongoc_read_concern_get_level (mongoc_collection_get_read_concern (gridfs->files)));
   ASSERT_CMPINT (mongoc_write_concern_get_w (mongoc_collection_get_write_concern (gridfs->chunks)),
                  ==,
                  MONGOC_WRITE_CONCERN_W_DEFAULT);
   ASSERT_CMPINT (mongoc_write_concern_get_w (mongoc_collection_get_write_concern (gridfs->files)),
                  ==,
                  MONGOC_WRITE_CONCERN_W_DEFAULT);
   mongoc_gridfs_bucket_destroy (gridfs);

   /* check out-of-range chunk sizes */
   gridfs = mongoc_gridfs_bucket_new (db, tmp_bson ("{'chunkSizeBytes': -1}"), NULL, &error);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "should be greater than 0");
   mongoc_gridfs_bucket_destroy (gridfs);

   gridfs = mongoc_gridfs_bucket_new (db, tmp_bson ("{'chunkSizeBytes': 2147483648}"), NULL, &error);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "out of range");
   mongoc_gridfs_bucket_destroy (gridfs);

   rc = mongoc_read_concern_new ();
   mongoc_read_concern_set_level (rc, MONGOC_READ_CONCERN_LEVEL_AVAILABLE);
   wc = mongoc_write_concern_new ();
   mongoc_write_concern_set_w (wc, MONGOC_WRITE_CONCERN_W_UNACKNOWLEDGED);

   opts = BCON_NEW ("bucketName", "abc", "chunkSizeBytes", BCON_INT32 (123));
   BSON_ASSERT (mongoc_read_concern_append (rc, opts));
   BSON_ASSERT (mongoc_write_concern_append (wc, opts));
   gridfs = mongoc_gridfs_bucket_new (db, opts, NULL, &error);
   ASSERT_OR_PRINT (gridfs, error);
   ASSERT_CMPSTR (gridfs->bucket_name, "abc");
   ASSERT_CMPINT32 (gridfs->chunk_size, ==, 123);
   ASSERT_CMPSTR (mongoc_read_concern_get_level (mongoc_collection_get_read_concern (gridfs->chunks)),
                  MONGOC_READ_CONCERN_LEVEL_AVAILABLE);
   ASSERT_CMPSTR (mongoc_read_concern_get_level (mongoc_collection_get_read_concern (gridfs->files)),
                  MONGOC_READ_CONCERN_LEVEL_AVAILABLE);
   ASSERT_CMPINT (mongoc_write_concern_get_w (mongoc_collection_get_write_concern (gridfs->chunks)),
                  ==,
                  MONGOC_WRITE_CONCERN_W_UNACKNOWLEDGED);
   ASSERT_CMPINT (mongoc_write_concern_get_w (mongoc_collection_get_write_concern (gridfs->files)),
                  ==,
                  MONGOC_WRITE_CONCERN_W_UNACKNOWLEDGED);
   mongoc_read_concern_destroy (rc);
   mongoc_write_concern_destroy (wc);
   bson_destroy (opts);
   mongoc_gridfs_bucket_destroy (gridfs);

   /* check validation of long bucket names */
   bucket_name = bson_malloc0 (128);
   memset (bucket_name, 'a', 128 - strlen ("chunks"));
   opts = BCON_NEW ("bucketName", bucket_name);
   gridfs = mongoc_gridfs_bucket_new (db, opts, NULL, &error);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "must have fewer");
   bson_destroy (opts);
   mongoc_gridfs_bucket_destroy (gridfs);

   /* one character shorter should be okay though. */
   *(bucket_name + ((int) (128 - strlen ("chunks") - 1))) = '\0';
   opts = BCON_NEW ("bucketName", bucket_name);
   gridfs = mongoc_gridfs_bucket_new (db, opts, NULL, &error);
   ASSERT_OR_PRINT (gridfs, error);
   bson_destroy (opts);
   mongoc_gridfs_bucket_destroy (gridfs);

   bson_free (bucket_name);
   mongoc_database_destroy (db);
   mongoc_client_destroy (client);
}

void
test_gridfs_bucket_install (TestSuite *suite)
{
   test_all_spec_tests (suite);
   TestSuite_AddLive (suite, "/gridfs/create_bucket", test_create_bucket);
   TestSuite_AddLive (suite, "/gridfs/upload_and_download", test_upload_and_download);
   TestSuite_AddFull (suite, "/gridfs/upload_error", test_upload_error, NULL, NULL, test_framework_skip_if_no_auth);
   TestSuite_AddFull (suite,
                      "/gridfs/find_w_session",
                      test_find_w_session,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_sessions,
                      test_framework_skip_if_no_crypto);
   TestSuite_AddFull (suite,
                      "/gridfs/find",
                      test_find,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_sessions,
                      test_framework_skip_if_no_crypto);
   TestSuite_AddLive (suite, "/gridfs/options", test_gridfs_bucket_opts);
}
