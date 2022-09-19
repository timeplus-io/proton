DROP STREAM IF EXISTS uncomparable_keys;

create stream foo (id uint64, key aggregate_function(max, uint64)) ENGINE MergeTree ORDER BY key; --{serverError 549}

create stream foo (id uint64, key aggregate_function(max, uint64)) ENGINE MergeTree PARTITION BY key; --{serverError 549}

create stream foo (id uint64, key aggregate_function(max, uint64)) ENGINE MergeTree ORDER BY (key) SAMPLE BY key; --{serverError 549}

DROP STREAM IF EXISTS uncomparable_keys;
