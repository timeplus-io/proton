/*
 * Copyright 2015 MongoDB Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 *
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

#include <inttypes.h> // PRIu16

#include "mongoc-host-list-private.h"
/* strcasecmp on windows */
#include "mongoc-util-private.h"
#include "utlist.h"

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_host_list_push --
 *
 *       Add a host to the front of the list and return it.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
mongoc_host_list_t *
_mongoc_host_list_push (const char *host, uint16_t port, int family, mongoc_host_list_t *next)
{
   mongoc_host_list_t *h;

   BSON_ASSERT (host);

   h = bson_malloc0 (sizeof (mongoc_host_list_t));
   bson_strncpy (h->host, host, sizeof h->host);
   h->port = port;
   bson_snprintf (h->host_and_port, sizeof h->host_and_port, "%s:%hu", host, port);

   h->family = family;
   h->next = next;

   return h;
}

static mongoc_host_list_t *
_mongoc_host_list_find_host_and_port (mongoc_host_list_t *hosts, const char *host_and_port)
{
   mongoc_host_list_t *iter;
   LL_FOREACH (hosts, iter)
   {
      BSON_ASSERT (iter);

      if (strcasecmp (iter->host_and_port, host_and_port) == 0) {
         return iter;
      }
   }

   return NULL;
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_host_list_upsert --
 *
 *       If new_host is not already in list, copy and add it to the end of list.
 *       If *list == NULL, then it will be set to a new host.
 *
 * Returns:
 *       Nothing.
 *
 *--------------------------------------------------------------------------
 */
void
_mongoc_host_list_upsert (mongoc_host_list_t **list, const mongoc_host_list_t *new_host)
{
   mongoc_host_list_t *link = NULL;
   mongoc_host_list_t *next_link = NULL;

   BSON_ASSERT (list);
   if (!new_host) {
      return;
   }

   link = _mongoc_host_list_find_host_and_port (*list, new_host->host_and_port);

   if (!link) {
      link = bson_malloc0 (sizeof (mongoc_host_list_t));
      LL_APPEND (*list, link);
   } else {
      /* Make sure linking is preserved when copying data into final. */
      next_link = link->next;
   }

   memcpy (link, new_host, sizeof (mongoc_host_list_t));
   link->next = next_link;
}


/* Duplicates a host list.
 */
mongoc_host_list_t *
_mongoc_host_list_copy_all (const mongoc_host_list_t *src)
{
   mongoc_host_list_t *tail = NULL;
   const mongoc_host_list_t *src_iter;
   mongoc_host_list_t *head = NULL;

   LL_FOREACH (src, src_iter)
   {
      tail = bson_malloc0 (sizeof (mongoc_host_list_t));
      memcpy (tail, src_iter, sizeof (mongoc_host_list_t));

      LL_PREPEND (head, tail);
   }

   return head;
}

size_t
_mongoc_host_list_length (const mongoc_host_list_t *list)
{
   const mongoc_host_list_t *tmp;
   size_t counter = 0u;

   tmp = list;
   while (tmp) {
      tmp = tmp->next;
      counter++;
   }

   return counter;
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_host_list_compare_one --
 *
 *       Check two hosts have the same domain (case-insensitive), port,
 *       and address family.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
bool
_mongoc_host_list_compare_one (const mongoc_host_list_t *host_a, const mongoc_host_list_t *host_b)
{
   return (0 == strcasecmp (host_a->host_and_port, host_b->host_and_port) && host_a->family == host_b->family);
}

bool
_mongoc_host_list_contains_one (mongoc_host_list_t *host_list, mongoc_host_list_t *host)
{
   return NULL != _mongoc_host_list_find_host_and_port (host_list, host->host_and_port);
}


/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_host_list_destroy_all --
 *
 *       Destroy whole linked list of hosts.
 *
 *--------------------------------------------------------------------------
 */
void
_mongoc_host_list_destroy_all (mongoc_host_list_t *host)
{
   mongoc_host_list_t *tmp;

   while (host) {
      tmp = host->next;
      bson_free (host);
      host = tmp;
   }
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_host_list_from_string --
 *
 *       Populate a mongoc_host_list_t from a fully qualified address
 *
 *--------------------------------------------------------------------------
 */
bool
_mongoc_host_list_from_string (mongoc_host_list_t *link_, const char *address)
{
   bson_error_t error = {0};
   bool r = _mongoc_host_list_from_string_with_err (link_, address, &error);
   if (!r) {
      MONGOC_ERROR ("Could not parse address %s: %s", address, error.message);
      return false;
   }
   return true;
}

bool
_mongoc_host_list_from_string_with_err (mongoc_host_list_t *link_, const char *address, bson_error_t *error)
{
   char *close_bracket;
   char *sport;
   uint16_t port;
   char *host;
   bool ret;
   bool ipv6 = false;

   close_bracket = strchr (address, ']');

   /* if this is an ipv6 address. */
   if (close_bracket) {
      /* if present, the port should immediately follow after ] */
      sport = strchr (close_bracket, ':');
      if (sport > close_bracket + 1) {
         bson_set_error (error,
                         MONGOC_ERROR_COMMAND,
                         MONGOC_ERROR_COMMAND_INVALID_ARG,
                         "If present, port should immediately follow the \"]\""
                         "in an IPv6 address");
         return false;
      }

      /* otherwise ] should be the last char. */
      if (!sport && *(close_bracket + 1) != '\0') {
         bson_set_error (error,
                         MONGOC_ERROR_COMMAND,
                         MONGOC_ERROR_COMMAND_INVALID_ARG,
                         "If port is not supplied, \"[\" should be the last"
                         "character");
         return false;
      }

      if (*address != '[') {
         bson_set_error (
            error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Missing matching bracket \"[\"");
         return false;
      }

      ipv6 = true;
   }
   /* otherwise, just find the first : */
   else {
      sport = strchr (address, ':');
   }

   /* like "example.com:27019" or "[fe80::1]:27019", but not "[fe80::1]" */
   if (sport) {
      if (sport == address) {
         /* bad address like ":27017" */
         bson_set_error (error,
                         MONGOC_ERROR_COMMAND,
                         MONGOC_ERROR_COMMAND_INVALID_ARG,
                         "Bad address, \":\" should not be first character");
         return false;
      }

      if (!mongoc_parse_port (&port, sport + 1)) {
         bson_set_error (error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "Port could not be parsed");
         return false;
      }

      /* if this is an ipv6 address, strip the [ and ] */
      if (ipv6) {
         host = bson_strndup (address + 1, close_bracket - address - 1);
      } else {
         host = bson_strndup (address, sport - address);
      }
   } else {
      /* if this is an ipv6 address, strip the [ and ] */
      if (ipv6) {
         host = bson_strndup (address + 1, close_bracket - address - 1);
      } else {
         host = bson_strdup (address);
      }
      port = MONGOC_DEFAULT_PORT;
   }

   ret = _mongoc_host_list_from_hostport_with_err (link_, host, port, error);

   bson_free (host);

   return ret;
}

bool
_mongoc_host_list_from_hostport_with_err (mongoc_host_list_t *link_,
                                          const char *host,
                                          uint16_t port,
                                          bson_error_t *error)
{
   BSON_ASSERT (host);
   BSON_ASSERT (link_);
   size_t host_len = strlen (host);
   *link_ = (mongoc_host_list_t){
      .next = NULL,
      .port = port,
   };

   if (host_len == 0) {
      bson_set_error (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_NAME_RESOLUTION, "Empty hostname in URI");
      return false;
   }

   if (host_len > BSON_HOST_NAME_MAX) {
      bson_set_error (error,
                      MONGOC_ERROR_STREAM,
                      MONGOC_ERROR_STREAM_NAME_RESOLUTION,
                      "Hostname provided in URI is too long, max is %d chars",
                      BSON_HOST_NAME_MAX);
      return false;
   }

   bson_strncpy (link_->host, host, host_len + 1);

   /* like "fe80::1" or "::1" */
   if (strchr (host, ':')) {
      link_->family = AF_INET6;

      // Check that IPv6 literal is two less than the max to account for `[` and
      // `]` added below.
      if (host_len > BSON_HOST_NAME_MAX - 2) {
         bson_set_error (error,
                         MONGOC_ERROR_STREAM,
                         MONGOC_ERROR_STREAM_NAME_RESOLUTION,
                         "IPv6 literal provided in URI is too long, max is %d chars",
                         BSON_HOST_NAME_MAX - 2);
         return false;
      }

      mongoc_lowercase (link_->host, link_->host);
      int req =
         bson_snprintf (link_->host_and_port, sizeof link_->host_and_port, "[%s]:%" PRIu16, link_->host, link_->port);
      BSON_ASSERT (bson_in_range_size_t_signed (req));
      // Use `<`, not `<=` to account for NULL byte.
      BSON_ASSERT ((size_t) req < sizeof link_->host_and_port);
   } else if (strchr (host, '/') && strstr (host, ".sock")) {
      link_->family = AF_UNIX;
      bson_strncpy (link_->host_and_port, link_->host, host_len + 1);
   } else {
      /* This is either an IPv4 or hostname. */
      link_->family = AF_UNSPEC;

      mongoc_lowercase (link_->host, link_->host);
      int req =
         bson_snprintf (link_->host_and_port, sizeof link_->host_and_port, "%s:%" PRIu16, link_->host, link_->port);
      BSON_ASSERT (bson_in_range_size_t_signed (req));
      // Use `<`, not `<=` to account for NULL byte.
      BSON_ASSERT ((size_t) req < sizeof link_->host_and_port);
   }

   return true;
}

void
_mongoc_host_list_remove_host (mongoc_host_list_t **hosts, const char *host, uint16_t port)
{
   mongoc_host_list_t *current;
   mongoc_host_list_t *prev = NULL;

   for (current = *hosts; current; prev = current, current = current->next) {
      if ((current->port == port) && (strcmp (current->host, host) == 0)) {
         /* Node found, unlink. */
         if (prev) {
            prev->next = current->next;
         } else {
            /* No previous, unlinking at head. */
            *hosts = current->next;
         }
         bson_free (current);
         break;
      }
   }
}
