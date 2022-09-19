select * from system.settings where name = 'send_timeout';
select * from system.merge_tree_settings order by length(description) limit 1;

with [
    'Seconds',
    'Bool',
    'int64',
    'string',
    'Char',
    'LogsLevel',
    'URI',
    'Float',
    'uint64',
    'MaxThreads',
    'Milliseconds',
    'JoinStrictness',
    'JoinAlgorithm',
    'OverflowMode',
    'TotalsMode',
    'LoadBalancing',
    'OverflowModeGroupBy',
    'DateTimeInputFormat',
    'DistributedProductMode'
] as types select hasAll( array_distinct(group_array(type)), types) from system.settings;

with [
    'Seconds',
    'Bool',
    'int64',
    'string',
    'Float',
    'uint64',
    'MaxThreads'
] as types select hasAll( array_distinct(group_array(type)), types) from system.merge_tree_settings;
