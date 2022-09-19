-- Tags: no-replicated-database, no-parallel
-- Tag no-replicated-database: Unsupported type of ALTER query

DROP STREAM IF EXISTS log_for_alter;

create stream log_for_alter (
  id uint64,
  Data string
)  ();

ALTER STREAM log_for_alter MODIFY SETTING aaa=123; -- { serverError 36 }

DROP STREAM IF EXISTS log_for_alter;

DROP STREAM IF EXISTS table_for_alter;

create stream table_for_alter (
  id uint64,
  Data string
) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity=4096;

ALTER STREAM table_for_alter MODIFY SETTING index_granularity=555; -- { serverError 472 }

SHOW create stream table_for_alter;

ALTER STREAM table_for_alter MODIFY SETTING  parts_to_throw_insert = 1, parts_to_delay_insert = 1;

SHOW create stream table_for_alter;

INSERT INTO table_for_alter VALUES (1, '1');
INSERT INTO table_for_alter VALUES (2, '2'); -- { serverError 252 }

DETACH TABLE table_for_alter;

ATTACH TABLE table_for_alter;

INSERT INTO table_for_alter VALUES (2, '2'); -- { serverError 252 }

ALTER STREAM table_for_alter MODIFY SETTING xxx_yyy=124; -- { serverError 115 }

ALTER STREAM table_for_alter MODIFY SETTING parts_to_throw_insert = 100, parts_to_delay_insert = 100;

INSERT INTO table_for_alter VALUES (2, '2');

SHOW create stream table_for_alter;

SELECT COUNT() FROM table_for_alter;

ALTER STREAM table_for_alter MODIFY SETTING check_delay_period=10, check_delay_period=20, check_delay_period=30;

SHOW create stream table_for_alter;

ALTER STREAM table_for_alter ADD COLUMN Data2 uint64, MODIFY SETTING check_delay_period=5, check_delay_period=10, check_delay_period=15;

SHOW create stream table_for_alter;

DROP STREAM IF EXISTS table_for_alter;


DROP STREAM IF EXISTS table_for_reset_setting;

create stream table_for_reset_setting (
 id uint64,
 Data string
) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity=4096;

ALTER STREAM table_for_reset_setting MODIFY SETTING index_granularity=555; -- { serverError 472 }

SHOW create stream table_for_reset_setting;

INSERT INTO table_for_reset_setting VALUES (1, '1');
INSERT INTO table_for_reset_setting VALUES (2, '2');

ALTER STREAM table_for_reset_setting MODIFY SETTING  parts_to_throw_insert = 1, parts_to_delay_insert = 1;

SHOW create stream table_for_reset_setting;

INSERT INTO table_for_reset_setting VALUES (1, '1'); -- { serverError 252 }

ALTER STREAM table_for_reset_setting RESET SETTING parts_to_delay_insert, parts_to_throw_insert;

SHOW create stream table_for_reset_setting;

INSERT INTO table_for_reset_setting VALUES (1, '1');
INSERT INTO table_for_reset_setting VALUES (2, '2');

DETACH TABLE table_for_reset_setting;
ATTACH TABLE table_for_reset_setting;

SHOW create stream table_for_reset_setting;

ALTER STREAM table_for_reset_setting RESET SETTING index_granularity; -- { serverError 472 }

-- ignore undefined setting
ALTER STREAM table_for_reset_setting RESET SETTING merge_with_ttl_timeout, unknown_setting;

ALTER STREAM table_for_reset_setting MODIFY SETTING merge_with_ttl_timeout = 300, max_concurrent_queries = 1;

SHOW create stream table_for_reset_setting;

ALTER STREAM table_for_reset_setting RESET SETTING max_concurrent_queries, merge_with_ttl_timeout;

SHOW create stream table_for_reset_setting;

DROP STREAM IF EXISTS table_for_reset_setting;