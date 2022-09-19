DROP STREAM IF EXISTS ttl_old_syntax;

create stream ttl_old_syntax (d date, i int) ENGINE = MergeTree(d, i, 8291);
ALTER STREAM ttl_old_syntax MODIFY TTL to_date('2020-01-01'); -- { serverError 36 }

DROP STREAM ttl_old_syntax;
