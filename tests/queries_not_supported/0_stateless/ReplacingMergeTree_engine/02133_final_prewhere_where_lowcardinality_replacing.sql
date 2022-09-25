DROP STREAM IF EXISTS errors_local;

create stream errors_local (level low_cardinality(string)) ENGINE=ReplacingMergeTree ORDER BY level settings min_bytes_for_wide_part = '10000000';
insert into errors_local select to_string(number) from numbers(10000);

SELECT to_type_name(level) FROM errors_local FINAL PREWHERE isNotNull(level) WHERE isNotNull(level) LIMIT 1;

DROP STREAM errors_local;

create stream errors_local(level low_cardinality(string)) ENGINE=ReplacingMergeTree ORDER BY level;
insert into errors_local select to_string(number) from numbers(10000);

SELECT to_type_name(level) FROM errors_local FINAL PREWHERE isNotNull(level) WHERE isNotNull(level) LIMIT 1;

DROP STREAM errors_local;
