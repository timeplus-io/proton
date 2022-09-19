DROP STREAM IF EXISTS prop_table;

create stream prop_table
(
    column_default uint64 DEFAULT 42,
    column_materialized uint64 MATERIALIZED column_default * 42,
    column_alias uint64 ALIAS column_default + 1,
    column_codec string CODEC(ZSTD(10)),
    column_comment date COMMENT 'Some comment',
    column_ttl uint64 TTL column_comment + INTERVAL 1 MONTH
)
ENGINE MergeTree()
ORDER BY tuple()
TTL column_comment + INTERVAL 2 MONTH;

SHOW create stream prop_table;

SYSTEM STOP TTL MERGES prop_table;

INSERT INTO prop_table (column_codec, column_comment, column_ttl) VALUES ('str', to_date('2019-10-01'), 1);

SELECT column_default, column_materialized, column_alias, column_codec, column_comment, column_ttl FROM prop_table;

ALTER STREAM prop_table MODIFY COLUMN column_comment REMOVE COMMENT;

SHOW create stream prop_table;

ALTER STREAM prop_table MODIFY COLUMN column_codec REMOVE CODEC;

SHOW create stream prop_table;

ALTER STREAM prop_table MODIFY COLUMN column_alias REMOVE ALIAS;

SELECT column_default, column_materialized, column_alias, column_codec, column_comment, column_ttl FROM prop_table;

SHOW create stream prop_table;

INSERT INTO prop_table (column_alias, column_codec, column_comment, column_ttl) VALUES (33, 'trs', to_date('2020-01-01'), 2);

SELECT column_default, column_materialized, column_alias, column_codec, column_comment, column_ttl FROM prop_table ORDER BY column_ttl;

ALTER STREAM prop_table MODIFY COLUMN column_materialized REMOVE MATERIALIZED;

SHOW create stream prop_table;

INSERT INTO prop_table (column_materialized, column_alias, column_codec, column_comment, column_ttl) VALUES (11, 44, 'rts', to_date('2020-02-01'), 3);

SELECT column_default, column_materialized, column_alias, column_codec, column_comment, column_ttl FROM prop_table ORDER BY column_ttl;

ALTER STREAM prop_table MODIFY COLUMN column_default REMOVE DEFAULT;

SHOW create stream prop_table;

INSERT INTO prop_table (column_materialized, column_alias, column_codec, column_comment, column_ttl) VALUES (22, 55, 'tsr', to_date('2020-03-01'), 4);

SELECT column_default, column_materialized, column_alias, column_codec, column_comment, column_ttl FROM prop_table ORDER BY column_ttl;

ALTER STREAM prop_table REMOVE TTL;

SHOW create stream prop_table;

ALTER STREAM prop_table MODIFY COLUMN column_ttl REMOVE TTL;

SHOW create stream prop_table;

SYSTEM START TTL MERGES prop_table;

OPTIMIZE STREAM prop_table FINAL;

SELECT COUNT() FROM prop_table;

DROP STREAM IF EXISTS prop_table;
