-- Tags: no-s3-storage
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
    'float',
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
] as types select hasAll(arrayDistinct(group_array(type)), types) from system.settings;

with [
    'Seconds',
    'Bool',
    'int64',
    'string',
    'float',
    'uint64',
    'MaxThreads'
] as types select hasAll(arrayDistinct(group_array(type)), types) from system.merge_tree_settings;
