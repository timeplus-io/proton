DROP STREAM IF EXISTS ttl_old_syntax;


CREATE STREAM ttl_old_syntax (d Date, i int) ENGINE = MergeTree(d, i, 8291);
ALTER STREAM ttl_old_syntax MODIFY TTL to_date('2020-01-01'); -- { serverError 36 }

DROP STREAM ttl_old_syntax;
