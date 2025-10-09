/*
 * Copyright 2016 MongoDB, Inc.
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

#include "mongoc-topology-description-apm-private.h"
#include "mongoc-server-description-private.h"

/* Application Performance Monitoring for topology events, complies with the
 * SDAM Monitoring Spec:

https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring-monitoring.rst

 */

/* ServerOpeningEvent */
void
_mongoc_topology_description_monitor_server_opening (const mongoc_topology_description_t *td,
                                                     mongoc_server_description_t *sd)
{
   if (td->apm_callbacks.server_opening && !sd->opened) {
      mongoc_apm_server_opening_t event;

      bson_oid_copy (&td->topology_id, &event.topology_id);
      event.host = &sd->host;
      event.context = td->apm_context;
      sd->opened = true;
      td->apm_callbacks.server_opening (&event);
   }
}

/* ServerDescriptionChangedEvent */
void
_mongoc_topology_description_monitor_server_changed (const mongoc_topology_description_t *td,
                                                     const mongoc_server_description_t *prev_sd,
                                                     const mongoc_server_description_t *new_sd)
{
   if (td->apm_callbacks.server_changed) {
      mongoc_apm_server_changed_t event;

      /* address is same in previous and new sd */
      bson_oid_copy (&td->topology_id, &event.topology_id);
      event.host = &new_sd->host;
      event.previous_description = prev_sd;
      event.new_description = new_sd;
      event.context = td->apm_context;
      td->apm_callbacks.server_changed (&event);
   }
}

/* ServerClosedEvent */
void
_mongoc_topology_description_monitor_server_closed (const mongoc_topology_description_t *td,
                                                    const mongoc_server_description_t *sd)
{
   if (td->apm_callbacks.server_closed) {
      mongoc_apm_server_closed_t event;

      bson_oid_copy (&td->topology_id, &event.topology_id);
      event.host = &sd->host;
      event.context = td->apm_context;
      td->apm_callbacks.server_closed (&event);
   }
}


/* Send TopologyOpeningEvent when first called on this topology description.
 * td is not const: we set its "opened" field here */
void
_mongoc_topology_description_monitor_opening (mongoc_topology_description_t *td)
{
   mongoc_topology_description_t *prev_td = NULL;
   mongoc_server_description_t *sd;

   if (td->opened) {
      return;
   }

   if (td->apm_callbacks.topology_changed) {
      /* prepare to call monitor_changed */
      prev_td = BSON_ALIGNED_ALLOC0 (mongoc_topology_description_t);
      mongoc_topology_description_init (prev_td, td->heartbeat_msec);
   }

   td->opened = true;

   if (td->apm_callbacks.topology_opening) {
      mongoc_apm_topology_opening_t event;

      bson_oid_copy (&td->topology_id, &event.topology_id);
      event.context = td->apm_context;
      td->apm_callbacks.topology_opening (&event);
   }

   if (td->apm_callbacks.topology_changed) {
      /* send initial description-changed event */
      _mongoc_topology_description_monitor_changed (prev_td, td);
   }

   for (size_t i = 0u; i < mc_tpld_servers (td)->items_len; i++) {
      sd = mongoc_set_get_item (mc_tpld_servers (td), i);
      _mongoc_topology_description_monitor_server_opening (td, sd);
   }

   /* If this is a load balanced topology:
    * - update the one server description to be LoadBalancer
    * - emit a server changed event Unknown => LoadBalancer
    * - emit a topology changed event
    */
   if (td->type == MONGOC_TOPOLOGY_LOAD_BALANCED) {
      mongoc_server_description_t *prev_sd;

      /* LoadBalanced deployments must have exactly one host listed. Otherwise,
       * an error would have occurred when constructing the topology. */
      BSON_ASSERT (mc_tpld_servers (td)->items_len == 1);
      sd = mongoc_set_get_item (mc_tpld_servers (td), 0);
      prev_sd = mongoc_server_description_new_copy (sd);
      BSON_ASSERT (prev_sd);
      if (td->apm_callbacks.topology_changed) {
         mongoc_topology_description_cleanup (prev_td);
         _mongoc_topology_description_copy_to (td, prev_td);
      }
      sd->type = MONGOC_SERVER_LOAD_BALANCER;
      _mongoc_topology_description_monitor_server_changed (td, prev_sd, sd);
      mongoc_server_description_destroy (prev_sd);
      if (td->apm_callbacks.topology_changed) {
         _mongoc_topology_description_monitor_changed (prev_td, td);
      }
   }

   if (prev_td) {
      mongoc_topology_description_cleanup (prev_td);
      bson_free (prev_td);
   }
}

/* TopologyDescriptionChangedEvent */
void
_mongoc_topology_description_monitor_changed (const mongoc_topology_description_t *prev_td,
                                              const mongoc_topology_description_t *new_td)
{
   if (new_td->apm_callbacks.topology_changed) {
      mongoc_apm_topology_changed_t event;

      /* callbacks, context, and id are the same in previous and new td */
      bson_oid_copy (&new_td->topology_id, &event.topology_id);
      event.context = new_td->apm_context;
      event.previous_description = prev_td;
      event.new_description = new_td;

      new_td->apm_callbacks.topology_changed (&event);
   }
}

/* TopologyClosedEvent */
void
_mongoc_topology_description_monitor_closed (const mongoc_topology_description_t *td)
{
   if (td->apm_callbacks.topology_closed) {
      mongoc_apm_topology_closed_t event;

      if (td->type == MONGOC_TOPOLOGY_LOAD_BALANCED) {
         const mongoc_server_description_t *sd;

         /* LoadBalanced deployments must have exactly one host listed. */
         BSON_ASSERT (mc_tpld_servers_const (td)->items_len == 1);
         sd = mongoc_set_get_item_const (mc_tpld_servers_const (td), 0);
         _mongoc_topology_description_monitor_server_closed (td, sd);
      }
      bson_oid_copy (&td->topology_id, &event.topology_id);
      event.context = td->apm_context;
      td->apm_callbacks.topology_closed (&event);
   }
}
