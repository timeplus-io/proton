DROP STREAM IF EXISTS main_table_01818;
DROP STREAM IF EXISTS tmp_table_01818;


create stream main_table_01818
(
    `id` uint32,
    `advertiser_id` string,
    `campaign_id` string,
    `name` string,
    `budget` float64,
    `budget_mode` string,
    `landing_type` string,
    `status` string,
    `modify_time` string,
    `campaign_type` string,
    `campaign_create_time` datetime,
    `campaign_modify_time` datetime,
    `create_time` datetime,
    `update_time` datetime
)
ENGINE = MergeTree
PARTITION BY advertiser_id
ORDER BY campaign_id
SETTINGS index_granularity = 8192;

create stream tmp_table_01818
(
    `id` uint32,
    `advertiser_id` string,
    `campaign_id` string,
    `name` string,
    `budget` float64,
    `budget_mode` string,
    `landing_type` string,
    `status` string,
    `modify_time` string,
    `campaign_type` string,
    `campaign_create_time` datetime,
    `campaign_modify_time` datetime,
    `create_time` datetime,
    `update_time` datetime
)
ENGINE = MergeTree
PARTITION BY advertiser_id
ORDER BY campaign_id
SETTINGS index_granularity = 8192;

SELECT 'INSERT INTO main_table_01818';
INSERT INTO main_table_01818 SELECT 1 as `id`, 'ClickHouse' as `advertiser_id`, * EXCEPT (`id`, `advertiser_id`)
FROM generateRandom(
    '`id` uint32,
    `advertiser_id` string,
    `campaign_id` string,
    `name` string,
    `budget` float64,
    `budget_mode` string,
    `landing_type` string,
    `status` string,
    `modify_time` string,
    `campaign_type` string,
    `campaign_create_time` datetime,
    `campaign_modify_time` datetime,
    `create_time` datetime,
    `update_time` datetime', 10, 10, 10) 
LIMIT 100;

SELECT 'INSERT INTO tmp_table_01818';
INSERT INTO tmp_table_01818 SELECT 2 as `id`, 'Database' as `advertiser_id`, * EXCEPT (`id`, `advertiser_id`)
FROM generateRandom(
    '`id` uint32,
    `advertiser_id` string,
    `campaign_id` string,
    `name` string,
    `budget` float64,
    `budget_mode` string,
    `landing_type` string,
    `status` string,
    `modify_time` string,
    `campaign_type` string,
    `campaign_create_time` datetime,
    `campaign_modify_time` datetime,
    `create_time` datetime,
    `update_time` datetime', 10, 10, 10) 
LIMIT 100;

SELECT 'INSERT INTO tmp_table_01818';
INSERT INTO tmp_table_01818 SELECT 3 as `id`, 'ClickHouse' as `advertiser_id`, * EXCEPT (`id`, `advertiser_id`)
FROM generateRandom(
    '`id` uint32,
    `advertiser_id` string,
    `campaign_id` string,
    `name` string,
    `budget` float64,
    `budget_mode` string,
    `landing_type` string,
    `status` string,
    `modify_time` string,
    `campaign_type` string,
    `campaign_create_time` datetime,
    `campaign_modify_time` datetime,
    `create_time` datetime,
    `update_time` datetime', 10, 10, 10) 
LIMIT 100;

SELECT 'ALL tmp_table_01818', count() FROM tmp_table_01818;
SELECT 'ALL main_table_01818', count() FROM main_table_01818;
SELECT 'tmp_table_01818', count() FROM tmp_table_01818 WHERE `advertiser_id` = 'ClickHouse';
SELECT 'main_table_01818', count() FROM main_table_01818 WHERE `advertiser_id` = 'ClickHouse';

SELECT 'Executing ALTER STREAM MOVE PARTITION...';
ALTER STREAM tmp_table_01818 MOVE PARTITION 'ClickHouse' TO TABLE main_table_01818;


SELECT 'ALL tmp_table_01818', count() FROM tmp_table_01818;
SELECT 'ALL main_table_01818', count() FROM main_table_01818;
SELECT 'tmp_table_01818', count() FROM tmp_table_01818 WHERE `advertiser_id` = 'ClickHouse';
SELECT 'main_table_01818', count() FROM main_table_01818 WHERE `advertiser_id` = 'ClickHouse';


DROP STREAM IF EXISTS main_table_01818;
DROP STREAM IF EXISTS tmp_table_01818;
