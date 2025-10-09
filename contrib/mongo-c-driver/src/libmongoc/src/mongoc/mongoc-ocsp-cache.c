/*
 * Copyright 2020-present MongoDB, Inc.
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


#include "mongoc-ocsp-cache-private.h"
#ifdef MONGOC_ENABLE_OCSP_OPENSSL

#include "utlist.h"
#include "mongoc-trace-private.h"
#include <bson/bson.h>
#include <common-thread-private.h>

typedef struct _cache_entry_list_t {
   struct _cache_entry_list_t *next;
   OCSP_CERTID *id;
   int cert_status, reason;
   ASN1_GENERALIZEDTIME *this_update, *next_update;
} cache_entry_list_t;

static cache_entry_list_t *cache;
static bson_mutex_t ocsp_cache_mutex;

void
_mongoc_ocsp_cache_init (void)
{
   bson_mutex_init (&ocsp_cache_mutex);
}

static int
cache_cmp (cache_entry_list_t *out, OCSP_CERTID *id)
{
   ENTRY;
   if (!out || !out->id || !id)
      RETURN (1);
   RETURN (OCSP_id_cmp (out->id, id));
}

static cache_entry_list_t *
get_cache_entry (OCSP_CERTID *id)
{
   cache_entry_list_t *iter = NULL;
   ENTRY;

   LL_SEARCH (cache, iter, id, cache_cmp);
   RETURN (iter);
}

#define REPLACE_ASN1_TIME(_old, _new)                                 \
   do {                                                               \
      if ((_new)) {                                                   \
         if ((_old))                                                  \
            ASN1_GENERALIZEDTIME_free ((_old));                       \
         (_old) = ASN1_item_dup (ASN1_ITEM_rptr (ASN1_TIME), (_new)); \
      }                                                               \
   } while (0)

static void
update_entry (cache_entry_list_t *entry,
              int cert_status,
              int reason,
              ASN1_GENERALIZEDTIME *this_update,
              ASN1_GENERALIZEDTIME *next_update)
{
   ENTRY;
   REPLACE_ASN1_TIME (entry->next_update, next_update);
   REPLACE_ASN1_TIME (entry->this_update, this_update);
   entry->cert_status = cert_status;
   entry->reason = reason;
}

#if OPENSSL_VERSION_NUMBER >= 0x10101000L
static int
_cmp_time (ASN1_TIME *a, ASN1_TIME *b)
{
   return ASN1_TIME_compare (a, b);
}
#else
static int
_cmp_time (ASN1_TIME *a, ASN1_TIME *b)
{
   /* For older OpenSSL, always report that "a" is before "b". I.e. do not
    * replace the entry.
    * If a driver would accept a stapled OCSP response and that response has a
    * later nextUpdate than the response already in the cache, drivers SHOULD
    * replace the older entry in the cache with the fresher response. */
   return -1;
}
#endif

void
_mongoc_ocsp_cache_set_resp (
   OCSP_CERTID *id, int cert_status, int reason, ASN1_GENERALIZEDTIME *this_update, ASN1_GENERALIZEDTIME *next_update)
{
   cache_entry_list_t *entry = NULL;
   ENTRY;

   bson_mutex_lock (&ocsp_cache_mutex);
   if (!(entry = get_cache_entry (id))) {
      entry = bson_malloc0 (sizeof (cache_entry_list_t));
      entry->id = OCSP_CERTID_dup (id);
      LL_APPEND (cache, entry);
      update_entry (entry, cert_status, reason, this_update, next_update);
   } else if (next_update && _cmp_time (next_update, entry->next_update) == 1) {
      update_entry (entry, cert_status, reason, this_update, next_update);
   } else {
      /* Do nothing; our next_update is at a later date */
   }
   bson_mutex_unlock (&ocsp_cache_mutex);
}

int
_mongoc_ocsp_cache_length (void)
{
   cache_entry_list_t *iter;
   int counter;

   bson_mutex_lock (&ocsp_cache_mutex);
   LL_COUNT (cache, iter, counter);
   bson_mutex_unlock (&ocsp_cache_mutex);
   RETURN (counter);
}

static void
cache_entry_destroy (cache_entry_list_t *entry)
{
   OCSP_CERTID_free (entry->id);
   ASN1_GENERALIZEDTIME_free (entry->this_update);
   ASN1_GENERALIZEDTIME_free (entry->next_update);
   bson_free (entry);
}
bool
_mongoc_ocsp_cache_get_status (OCSP_CERTID *id,
                               int *cert_status,
                               int *reason,
                               ASN1_GENERALIZEDTIME **this_update,
                               ASN1_GENERALIZEDTIME **next_update)
{
   cache_entry_list_t *entry = NULL;
   bool ret = false;
   ENTRY;

   bson_mutex_lock (&ocsp_cache_mutex);
   if (!(entry = get_cache_entry (id))) {
      GOTO (done);
   }

   if (entry->this_update && entry->next_update &&
       !OCSP_check_validity (entry->this_update, entry->next_update, 0L, -1L)) {
      LL_DELETE (cache, entry);
      cache_entry_destroy (entry);
      GOTO (done);
   }

   BSON_ASSERT_PARAM (cert_status);
   BSON_ASSERT_PARAM (reason);
   BSON_ASSERT_PARAM (this_update);
   BSON_ASSERT_PARAM (next_update);

   *cert_status = entry->cert_status;
   *reason = entry->reason;
   *this_update = entry->this_update;
   *next_update = entry->next_update;

   ret = true;
done:
   bson_mutex_unlock (&ocsp_cache_mutex);
   RETURN (ret);
}

void
_mongoc_ocsp_cache_cleanup (void)
{
   cache_entry_list_t *iter = NULL;
   cache_entry_list_t *next = NULL;
   ENTRY;

   bson_mutex_lock (&ocsp_cache_mutex);
   for (iter = cache; iter != NULL; iter = next) {
      next = iter->next;
      cache_entry_destroy (iter);
   }

   cache = NULL;
   bson_mutex_unlock (&ocsp_cache_mutex);
   bson_mutex_destroy (&ocsp_cache_mutex);
}

#endif /* MONGOC_ENABLE_OCSP_OPENSSL */
