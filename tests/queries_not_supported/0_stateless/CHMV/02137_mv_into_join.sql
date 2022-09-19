create stream main ( `id` string, `color` string, `section` string, `description` string) ENGINE = MergeTree ORDER BY tuple();
create stream destination_join ( `key` string, `id` string, `color` string, `section` string, `description` string) ENGINE = Join(ANY, LEFT, key);
create stream destination_set (`key` string) ENGINE = Set;

CREATE MATERIALIZED VIEW mv_to_join TO `destination_join` AS SELECT concat(id, '_', color) AS key, * FROM main;
CREATE MATERIALIZED VIEW mv_to_set TO `destination_set` AS SELECT key FROM destination_join;

INSERT INTO main VALUES ('sku_0001','black','women','nice shirt');
SELECT * FROM main;
SELECT * FROM destination_join;
SELECT * FROM destination_join WHERE key in destination_set;

DROP STREAM mv_to_set;
DROP STREAM destination_set;
DROP STREAM mv_to_join;
DROP STREAM destination_join;
DROP STREAM main;
