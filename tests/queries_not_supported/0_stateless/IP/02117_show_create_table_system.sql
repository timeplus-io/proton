/* we will `use system` to bypass style check,
because `show create stream` statement
cannot fit the requirement in check-sytle, which is as

"# Queries to:
tables_with_database_column=(
    system.tables
    system.parts
    system.detached_parts
    system.parts_columns
    system.columns
    system.projection_parts
    system.mutations
)
# should have database = currentDatabase() condition"

 */
use system;
show create stream aggregate_function_combinators;
show create stream asynchronous_inserts;
show create stream asynchronous_metrics;
show create stream build_options;
show create stream clusters;
show create stream collations;
show create stream columns;
show create stream contributors;
show create stream current_roles;
show create stream data_skipping_indices;
show create stream data_type_families;
show create stream databases;
show create stream detached_parts;
show create stream dictionaries;
show create stream disks;
show create stream distributed_ddl_queue;
show create stream distribution_queue;
show create stream enabled_roles;
show create stream errors;
show create stream events;
show create stream formats;
show create stream functions;
show create stream grants;
show create stream graphite_retentions;
show create stream licenses;
show create stream macros;
show create stream merge_tree_settings;
show create stream merges;
show create stream metrics;
show create stream models;
show create stream mutations;
show create stream numbers;
show create stream numbers_mt;
show create stream one;
show create stream part_moves_between_shards;
show create stream parts;
show create stream parts_columns;
show create stream privileges;
show create stream processes;
show create stream projection_parts;
show create stream projection_parts_columns;
show create stream quota_limits;
show create stream quota_usage;
show create stream quotas;
show create stream quotas_usage;
show create stream replicas;
show create stream replicated_fetches;
show create stream replicated_merge_tree_settings;
show create stream replication_queue;
show create stream role_grants;
show create stream roles;
show create stream row_policies;
show create stream settings;
show create stream settings_profile_elements;
show create stream settings_profiles;
show create stream stack_trace;
show create stream storage_policies;
show create stream table_engines;
show create stream table_functions;
show create stream tables;
show create stream time_zones;
show create stream user_directories;
show create stream users;
show create stream warnings;
show create stream zeros;
show create stream zeros_mt;
