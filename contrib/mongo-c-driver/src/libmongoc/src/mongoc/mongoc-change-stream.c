/*
 * Copyright 2017-present MongoDB, Inc.
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

#include <bson/bson.h>
#include "mongoc-cluster-private.h"
#include "mongoc-change-stream-private.h"
#include "mongoc-collection-private.h"
#include "mongoc-client-private.h"
#include "mongoc-client-session-private.h"
#include "mongoc-cursor-private.h"
#include "mongoc-database-private.h"
#include "mongoc-error.h"
#include "mongoc-error-private.h"

#define CHANGE_STREAM_ERR(_str) \
   bson_set_error (&stream->err, MONGOC_ERROR_CURSOR, MONGOC_ERROR_BSON, "Could not set " _str)

/* the caller knows either a client or server error has occurred.
 * `reply` contains the server reply or an empty document. */
static bool
_is_resumable_error (mongoc_change_stream_t *stream, const bson_t *reply)
{
   bson_error_t error = {0};

   /* Change Streams Spec resumable criteria: "any error encountered which is
    * not a server error (e.g. a timeout error or network error)" */
   if (bson_empty (reply)) {
      return true;
   }

   if (_mongoc_cmd_check_ok (reply, MONGOC_ERROR_API_VERSION_2, &error)) {
      return true;
   }

   if (error.code == MONGOC_SERVER_ERR_CURSOR_NOT_FOUND) {
      return true;
   }

   if (stream->max_wire_version >= WIRE_VERSION_4_4) {
      return mongoc_error_has_label (reply, "ResumableChangeStreamError");
   }

   switch (error.code) {
   case MONGOC_SERVER_ERR_HOSTUNREACHABLE:
   case MONGOC_SERVER_ERR_HOSTNOTFOUND:
   case MONGOC_SERVER_ERR_NETWORKTIMEOUT:
   case MONGOC_SERVER_ERR_SHUTDOWNINPROGRESS:
   case MONGOC_SERVER_ERR_PRIMARYSTEPPEDDOWN:
   case MONGOC_SERVER_ERR_EXCEEDEDTIMELIMIT:
   case MONGOC_SERVER_ERR_SOCKETEXCEPTION:
   case MONGOC_SERVER_ERR_NOTPRIMARY:
   case MONGOC_SERVER_ERR_INTERRUPTEDATSHUTDOWN:
   case MONGOC_SERVER_ERR_INTERRUPTEDDUETOREPLSTATECHANGE:
   case MONGOC_SERVER_ERR_NOTPRIMARYNOSECONDARYOK:
   case MONGOC_SERVER_ERR_NOTPRIMARYORSECONDARY:
   case MONGOC_SERVER_ERR_STALESHARDVERSION:
   case MONGOC_SERVER_ERR_STALEEPOCH:
   case MONGOC_SERVER_ERR_STALECONFIG:
   case MONGOC_SERVER_ERR_RETRYCHANGESTREAM:
   case MONGOC_SERVER_ERR_FAILEDTOSATISFYREADPREFERENCE:
      return true;
   default:
      return false;
   }
}


static void
_set_resume_token (mongoc_change_stream_t *stream, const bson_t *resume_token)
{
   BSON_ASSERT (stream);
   BSON_ASSERT (resume_token);

   bson_destroy (&stream->resume_token);
   bson_copy_to (resume_token, &stream->resume_token);
}


/* construct the aggregate command in cmd. looks like one of the following:
 * for a collection change stream:
 *   { aggregate: collname, pipeline: [], cursor: { batchSize: x } }
 * for a database change stream:
 *   { aggregate: 1, pipeline: [], cursor: { batchSize: x } }
 * for a client change stream:
 *   { aggregate: 1, pipeline: [{$changeStream: {allChangesForCluster: true}}],
 *     cursor: { batchSize: x } }
 */
static void
_make_command (mongoc_change_stream_t *stream, bson_t *command)
{
   bson_iter_t iter;
   bson_t change_stream_stage; /* { $changeStream: <change_stream_doc> } */
   bson_t change_stream_doc;
   bson_array_builder_t *pipeline;
   bson_t cursor_doc;

   if (stream->change_stream_type == MONGOC_CHANGE_STREAM_COLLECTION) {
      bson_append_utf8 (command, "aggregate", 9, stream->coll, (int) strlen (stream->coll));
   } else {
      bson_append_int32 (command, "aggregate", 9, 1);
   }

   bson_append_array_builder_begin (command, "pipeline", 8, &pipeline);

   /* append the $changeStream stage. */
   bson_array_builder_append_document_begin (pipeline, &change_stream_stage);
   bson_append_document_begin (&change_stream_stage, "$changeStream", 13, &change_stream_doc);
   if (stream->full_document) {
      bson_concat (&change_stream_doc, stream->full_document);
   }
   if (stream->full_document_before_change) {
      bson_concat (&change_stream_doc, stream->full_document_before_change);
   }
   if (stream->show_expanded_events) {
      BSON_APPEND_BOOL (&change_stream_doc, "showExpandedEvents", stream->show_expanded_events);
   }

   if (stream->resumed) {
      /* Change stream spec: Resume Process */
      /* If there is a cached resumeToken: */
      if (!bson_empty (&stream->resume_token)) {
         /* If the ChangeStream was started with startAfter
            and has yet to return a result document: */
         if (!bson_empty (&stream->opts.startAfter) && !stream->has_returned_results) {
            /* The driver MUST set startAfter to the cached resumeToken */
            BSON_APPEND_DOCUMENT (&change_stream_doc, "startAfter", &stream->resume_token);
         } else {
            /* The driver MUST set resumeAfter to the cached resumeToken */
            BSON_APPEND_DOCUMENT (&change_stream_doc, "resumeAfter", &stream->resume_token);
         }
      } else if (!_mongoc_timestamp_empty (&stream->operation_time) && stream->max_wire_version >= WIRE_VERSION_4_0) {
         /* Else if there is no cached resumeToken and the ChangeStream
            has a saved operation time and the max wire version is >= 7,
            the driver MUST set startAtOperationTime */
         _mongoc_timestamp_append (&stream->operation_time, &change_stream_doc, "startAtOperationTime");
      }
   } else {
      /* Change streams spec: "startAtOperationTime, resumeAfter, and startAfter
       * are all mutually exclusive; if any two are set, the server will return
       * an error. Drivers MUST NOT throw a custom error, and MUST defer to the
       * server error." */
      if (!bson_empty (&stream->opts.resumeAfter)) {
         BSON_APPEND_DOCUMENT (&change_stream_doc, "resumeAfter", &stream->opts.resumeAfter);

         /* Update the cached resume token */
         _set_resume_token (stream, &stream->opts.resumeAfter);
      }

      if (!bson_empty (&stream->opts.startAfter)) {
         BSON_APPEND_DOCUMENT (&change_stream_doc, "startAfter", &stream->opts.startAfter);

         /* Update the cached resume token (take precedence over resumeAfter) */
         _set_resume_token (stream, &stream->opts.startAfter);
      }

      if (!_mongoc_timestamp_empty (&stream->operation_time)) {
         _mongoc_timestamp_append (&stream->operation_time, &change_stream_doc, "startAtOperationTime");
      }
   }

   if (stream->change_stream_type == MONGOC_CHANGE_STREAM_CLIENT) {
      bson_append_bool (&change_stream_doc, "allChangesForCluster", 20, true);
   }
   bson_append_document_end (&change_stream_stage, &change_stream_doc);
   bson_array_builder_append_document_end (pipeline, &change_stream_stage);

   /* Append user pipeline if it exists */
   if (bson_iter_init_find (&iter, &stream->pipeline_to_append, "pipeline") && BSON_ITER_HOLDS_ARRAY (&iter)) {
      bson_iter_t child_iter;

      BSON_ASSERT (bson_iter_recurse (&iter, &child_iter));
      while (bson_iter_next (&child_iter)) {
         /* the user pipeline may consist of invalid stages or non-documents.
          * append anyway, and rely on the server error. */
         bson_array_builder_append_value (pipeline, bson_iter_value (&child_iter));
      }
   }

   bson_append_array_builder_end (command, pipeline);

   /* Add batch size if needed */
   bson_append_document_begin (command, "cursor", 6, &cursor_doc);
   if (stream->batch_size > 0) {
      bson_append_int32 (&cursor_doc, "batchSize", 9, stream->batch_size);
   }
   bson_append_document_end (command, &cursor_doc);
}

/*---------------------------------------------------------------------------
 *
 * _make_cursor --
 *
 *       Construct and send the aggregate command and create the resulting
 *       cursor. On error, stream->cursor remains NULL, otherwise it is
 *       created and must be destroyed.
 *
 * Return:
 *       False on error and sets stream->err.
 *
 *--------------------------------------------------------------------------
 */
static bool
_make_cursor (mongoc_change_stream_t *stream)
{
   mongoc_client_session_t *cs = NULL;
   bson_t command_opts;
   bson_t command; /* { aggregate: "coll", pipeline: [], ... } */
   bson_t reply;
   bson_t getmore_opts = BSON_INITIALIZER;
   bson_iter_t iter;
   mongoc_server_stream_t *server_stream;

   BSON_ASSERT (stream);
   BSON_ASSERT (!stream->cursor);
   bson_init (&command);
   bson_copy_to (&(stream->opts.extra), &command_opts);

   if (stream->opts.comment.value_type != BSON_TYPE_EOD) {
      bson_append_value (&command_opts, "comment", 7, &stream->opts.comment);
      bson_append_value (&getmore_opts, "comment", 7, &stream->opts.comment);
   }

   if (bson_iter_init_find (&iter, &command_opts, "sessionId")) {
      if (!_mongoc_client_session_from_iter (stream->client, &iter, &cs, &stream->err)) {
         goto cleanup;
      }
   } else if (stream->implicit_session) {
      /* If an implicit session was created before, and this cursor is now
       * being recreated after resuming, then use the same session as before. */
      cs = stream->implicit_session;
      if (!mongoc_client_session_append (cs, &command_opts, &stream->err)) {
         goto cleanup;
      }
   } else {
      /* Create an implicit session. This session lsid must be the same for the
       * agg command and the subsequent getMores. Thus, this implicit session is
       * passed as if it were an explicit session to
       * mongoc_client_read_command_with_opts and
       * _mongoc_cursor_change_stream_new, but it is still implicit and its
       * lifetime is owned by this change_stream_t. */
      mongoc_session_opt_t *session_opts;
      session_opts = mongoc_session_opts_new ();
      mongoc_session_opts_set_causal_consistency (session_opts, false);
      /* returns NULL if sessions aren't supported. ignore errors. */
      cs = mongoc_client_start_session (stream->client, session_opts, NULL);
      stream->implicit_session = cs;
      mongoc_session_opts_destroy (session_opts);
      if (cs && !mongoc_client_session_append (cs, &command_opts, &stream->err)) {
         goto cleanup;
      }
   }

   if (cs && !mongoc_client_session_append (cs, &getmore_opts, &stream->err)) {
      goto cleanup;
   }

   server_stream =
      mongoc_cluster_stream_for_reads (&stream->client->cluster, stream->read_prefs, cs, NULL, &reply, &stream->err);
   if (!server_stream) {
      bson_destroy (&stream->err_doc);
      bson_copy_to (&reply, &stream->err_doc);
      bson_destroy (&reply);
      goto cleanup;
   }
   bson_append_int32 (&command_opts, "serverId", 8, server_stream->sd->id);
   bson_append_int32 (&getmore_opts, "serverId", 8, server_stream->sd->id);
   stream->max_wire_version = server_stream->sd->max_wire_version;
   mongoc_server_stream_cleanup (server_stream);

   if (stream->read_concern && !bson_has_field (&command_opts, "readConcern")) {
      mongoc_read_concern_append (stream->read_concern, &command_opts);
   }

   _make_command (stream, &command);

   /* even though serverId has already been set, still pass the read prefs.
    * they are necessary for OP_MSG if sending to a secondary. */
   if (!mongoc_client_read_command_with_opts (
          stream->client, stream->db, &command, stream->read_prefs, &command_opts, &reply, &stream->err)) {
      bson_destroy (&stream->err_doc);
      bson_copy_to (&reply, &stream->err_doc);
      bson_destroy (&reply);
      goto cleanup;
   }

   bson_append_bool (&getmore_opts, MONGOC_CURSOR_TAILABLE, MONGOC_CURSOR_TAILABLE_LEN, true);
   bson_append_bool (&getmore_opts, MONGOC_CURSOR_AWAIT_DATA, MONGOC_CURSOR_AWAIT_DATA_LEN, true);

   /* maxTimeMS is only appended to getMores if these are set in cursor opts. */
   if (stream->max_await_time_ms > 0) {
      bson_append_int64 (&getmore_opts,
                         MONGOC_CURSOR_MAX_AWAIT_TIME_MS,
                         MONGOC_CURSOR_MAX_AWAIT_TIME_MS_LEN,
                         stream->max_await_time_ms);
   }

   if (stream->batch_size > 0) {
      bson_append_int32 (&getmore_opts, MONGOC_CURSOR_BATCH_SIZE, MONGOC_CURSOR_BATCH_SIZE_LEN, stream->batch_size);
   }

   /* steals reply. */
   stream->cursor = _mongoc_cursor_change_stream_new (stream->client, &reply, &getmore_opts);

   if (mongoc_cursor_error (stream->cursor, NULL)) {
      goto cleanup;
   }

   /* Change stream spec: "When aggregate or getMore returns: If an empty batch
    * was returned and a postBatchResumeToken was included, cache it." */
   if (_mongoc_cursor_change_stream_end_of_batch (stream->cursor) &&
       _mongoc_cursor_change_stream_has_post_batch_resume_token (stream->cursor)) {
      _set_resume_token (stream, _mongoc_cursor_change_stream_get_post_batch_resume_token (stream->cursor));
   }

   /* Change stream spec: startAtOperationTime */
   if (bson_empty (&stream->opts.resumeAfter) && bson_empty (&stream->opts.startAfter) &&
       _mongoc_timestamp_empty (&stream->operation_time) && stream->max_wire_version >= WIRE_VERSION_4_0 &&
       bson_empty (&stream->resume_token) &&
       bson_iter_init_find (&iter, _mongoc_cursor_change_stream_get_reply (stream->cursor), "operationTime") &&
       BSON_ITER_HOLDS_TIMESTAMP (&iter)) {
      _mongoc_timestamp_set_from_bson (&stream->operation_time, &iter);
   }

cleanup:
   bson_destroy (&command);
   bson_destroy (&command_opts);
   bson_destroy (&getmore_opts);
   return stream->err.code == 0;
}

/*---------------------------------------------------------------------------
 *
 * _change_stream_init --
 *
 *       Called after @stream has the collection name, database name, read
 *       preferences, and read concern set. Creates the change streams
 *       cursor.
 *
 *--------------------------------------------------------------------------
 */
void
_change_stream_init (mongoc_change_stream_t *stream, const bson_t *pipeline, const bson_t *opts)
{
   BSON_ASSERT (pipeline);

   stream->max_await_time_ms = -1;
   stream->batch_size = -1;
   bson_init (&stream->pipeline_to_append);
   bson_init (&stream->resume_token);
   bson_init (&stream->err_doc);

   if (!_mongoc_change_stream_opts_parse (stream->client, opts, &stream->opts, &stream->err)) {
      return;
   }

   if (stream->opts.fullDocument) {
      stream->full_document = BCON_NEW ("fullDocument", stream->opts.fullDocument);
   }

   if (stream->opts.fullDocumentBeforeChange) {
      stream->full_document_before_change =
         BCON_NEW ("fullDocumentBeforeChange", stream->opts.fullDocumentBeforeChange);
   }

   _mongoc_timestamp_set (&stream->operation_time, &stream->opts.startAtOperationTime);

   stream->batch_size = stream->opts.batchSize;
   stream->max_await_time_ms = stream->opts.maxAwaitTimeMS;
   stream->show_expanded_events = stream->opts.showExpandedEvents;

   /* Accept two forms of user pipeline:
    * 1. A document like: { "pipeline": [...] }
    * 2. An array-like document: { "0": {}, "1": {}, ... }
    * If the passed pipeline is invalid, we pass it along and let the server
    * error instead.
    */
   if (!bson_empty (pipeline)) {
      bson_iter_t iter;
      if (bson_iter_init_find (&iter, pipeline, "pipeline") && BSON_ITER_HOLDS_ARRAY (&iter)) {
         if (!BSON_APPEND_VALUE (&stream->pipeline_to_append, "pipeline", bson_iter_value (&iter))) {
            CHANGE_STREAM_ERR ("pipeline");
         }
      } else {
         if (!BSON_APPEND_ARRAY (&stream->pipeline_to_append, "pipeline", pipeline)) {
            CHANGE_STREAM_ERR ("pipeline");
         }
      }
   }

   if (stream->err.code == 0) {
      (void) _make_cursor (stream);
   }
}

mongoc_change_stream_t *
_mongoc_change_stream_new_from_collection (const mongoc_collection_t *coll, const bson_t *pipeline, const bson_t *opts)
{
   mongoc_change_stream_t *stream;
   BSON_ASSERT (coll);

   stream = BSON_ALIGNED_ALLOC0 (mongoc_change_stream_t);
   stream->db = bson_strdup (coll->db);
   stream->coll = bson_strdup (coll->collection);
   stream->read_prefs = mongoc_read_prefs_copy (coll->read_prefs);
   stream->read_concern = mongoc_read_concern_copy (coll->read_concern);
   stream->client = coll->client;
   stream->change_stream_type = MONGOC_CHANGE_STREAM_COLLECTION;
   _change_stream_init (stream, pipeline, opts);
   return stream;
}

mongoc_change_stream_t *
_mongoc_change_stream_new_from_database (const mongoc_database_t *db, const bson_t *pipeline, const bson_t *opts)
{
   mongoc_change_stream_t *stream;
   BSON_ASSERT (db);

   stream = BSON_ALIGNED_ALLOC0 (mongoc_change_stream_t);
   stream->db = bson_strdup (db->name);
   stream->coll = NULL;
   stream->read_prefs = mongoc_read_prefs_copy (db->read_prefs);
   stream->read_concern = mongoc_read_concern_copy (db->read_concern);
   stream->client = db->client;
   stream->change_stream_type = MONGOC_CHANGE_STREAM_DATABASE;
   _change_stream_init (stream, pipeline, opts);
   return stream;
}

mongoc_change_stream_t *
_mongoc_change_stream_new_from_client (mongoc_client_t *client, const bson_t *pipeline, const bson_t *opts)
{
   mongoc_change_stream_t *stream;
   BSON_ASSERT (client);

   stream = BSON_ALIGNED_ALLOC0 (mongoc_change_stream_t);
   stream->db = bson_strdup ("admin");
   stream->coll = NULL;
   stream->read_prefs = mongoc_read_prefs_copy (client->read_prefs);
   stream->read_concern = mongoc_read_concern_copy (client->read_concern);
   stream->client = client;
   stream->change_stream_type = MONGOC_CHANGE_STREAM_CLIENT;
   _change_stream_init (stream, pipeline, opts);
   return stream;
}


const bson_t *
mongoc_change_stream_get_resume_token (mongoc_change_stream_t *stream)
{
   if (!bson_empty (&stream->resume_token)) {
      return &stream->resume_token;
   }

   return NULL;
}


bool
mongoc_change_stream_next (mongoc_change_stream_t *stream, const bson_t **bson)
{
   bson_iter_t iter;
   bson_t doc_resume_token;
   uint32_t len;
   const uint8_t *data;
   bool ret = false;

   BSON_ASSERT (stream);
   BSON_ASSERT (bson);

   if (stream->err.code != 0) {
      goto end;
   }

   BSON_ASSERT (stream->cursor);
   if (!mongoc_cursor_next (stream->cursor, bson)) {
      const bson_t *err_doc;
      bson_error_t err;
      bool resumable = false;

      if (!mongoc_cursor_error_document (stream->cursor, &err, &err_doc)) {
         /* no error occurred, just no documents left. */
         goto end;
      }

      resumable = _is_resumable_error (stream, err_doc);
      while (resumable) {
         /* recreate the cursor. */
         mongoc_cursor_destroy (stream->cursor);
         stream->cursor = NULL;
         stream->resumed = true;
         if (!_make_cursor (stream)) {
            goto end;
         }
         if (mongoc_cursor_next (stream->cursor, bson)) {
            break;
         }
         if (!mongoc_cursor_error_document (stream->cursor, &err, &err_doc)) {
            goto end;
         }
         if (err_doc) {
            resumable = _is_resumable_error (stream, err_doc);
         } else {
            resumable = false;
         }
      }

      if (!resumable) {
         stream->err = err;
         bson_destroy (&stream->err_doc);
         bson_copy_to (err_doc, &stream->err_doc);
         goto end;
      }
   }

   /* we have received documents, either from the first call to next or after a
    * resume. */
   stream->has_returned_results = true;

   if (!bson_iter_init_find (&iter, *bson, "_id") || !BSON_ITER_HOLDS_DOCUMENT (&iter)) {
      bson_set_error (&stream->err,
                      MONGOC_ERROR_CURSOR,
                      MONGOC_ERROR_CHANGE_STREAM_NO_RESUME_TOKEN,
                      "Cannot provide resume functionality when the resume "
                      "token is missing");
      goto end;
   }

   /* copy the resume token. */
   bson_iter_document (&iter, &len, &data);
   BSON_ASSERT (bson_init_static (&doc_resume_token, data, len));
   _set_resume_token (stream, &doc_resume_token);

   /* clear out the operation time, since we no longer need it to resume. */
   _mongoc_timestamp_clear (&stream->operation_time);
   ret = true;

end:
   /* Change stream spec: Updating the Cached Resume Token */
   if (stream->cursor && !mongoc_cursor_error (stream->cursor, NULL) &&
       _mongoc_cursor_change_stream_end_of_batch (stream->cursor) &&
       _mongoc_cursor_change_stream_has_post_batch_resume_token (stream->cursor)) {
      _set_resume_token (stream, _mongoc_cursor_change_stream_get_post_batch_resume_token (stream->cursor));
   }


   /* Driver Sessions Spec: "When an implicit session is associated with a
    * cursor for use with getMore operations, the session MUST be returned to
    * the pool immediately following a getMore operation that indicates that the
    * cursor has been exhausted." */
   if (stream->implicit_session) {
      /* if creating the change stream cursor errored, it may be null. */
      if (!stream->cursor || stream->cursor->cursor_id == 0) {
         mongoc_client_session_destroy (stream->implicit_session);
         stream->implicit_session = NULL;
      }
   }
   return ret;
}

bool
mongoc_change_stream_error_document (const mongoc_change_stream_t *stream, bson_error_t *err, const bson_t **bson)
{
   BSON_ASSERT (stream);

   if (stream->err.code != 0) {
      if (err) {
         *err = stream->err;
      }
      if (bson) {
         *bson = &stream->err_doc;
      }
      return true;
   }

   if (bson) {
      *bson = NULL;
   }
   return false;
}

void
mongoc_change_stream_destroy (mongoc_change_stream_t *stream)
{
   if (!stream) {
      return;
   }

   bson_destroy (&stream->pipeline_to_append);
   bson_destroy (&stream->resume_token);
   bson_destroy (stream->full_document);
   bson_destroy (stream->full_document_before_change);
   bson_destroy (&stream->err_doc);
   _mongoc_change_stream_opts_cleanup (&stream->opts);
   mongoc_cursor_destroy (stream->cursor);
   mongoc_client_session_destroy (stream->implicit_session);
   mongoc_read_prefs_destroy (stream->read_prefs);
   mongoc_read_concern_destroy (stream->read_concern);

   bson_free (stream->db);
   bson_free (stream->coll);
   bson_free (stream);
}
