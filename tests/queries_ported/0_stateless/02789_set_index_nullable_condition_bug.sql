drop stream if exists test_table;
CREATE STREAM test_table
(
    col1 string,
    col2 string,
    INDEX test_table_col2_idx col2 TYPE set(0) GRANULARITY 1
) ENGINE = MergeTree()
      ORDER BY col1
AS SELECT 'v1', 'v2';

SELECT * FROM test_table
WHERE 1 == 1 AND col1 == col1 OR
       0 AND col2 == NULL;

drop stream if exists test_table;

-- Test for ClickHouse issue #75485 / proton-enterprise#11936:
-- `col OR col IS NULL` with a set skip index on a nullable column
-- must not prune granules that contain NULL values.

SELECT 'Bulk filtering enabled';
set secondary_indices_enable_bulk_filtering = 1;

drop stream if exists tab_bulk1;
create stream tab_bulk1(col nullable(bool), INDEX col_idx col TYPE set(0))
    settings flush_threshold_count=1;
insert into tab_bulk1(col) values (DEFAULT), (DEFAULT);
select sleep(1) format Null;
select count() from table(tab_bulk1) where col or col is null;
drop stream tab_bulk1;

drop stream if exists tab_bulk2;
create stream tab_bulk2(col nullable(bool), INDEX col_idx col TYPE set(0))
    settings flush_threshold_count=1;
insert into tab_bulk2(col) values (DEFAULT), (DEFAULT), (true);
select sleep(1) format Null;
select count() from table(tab_bulk2) where col or col is null;
drop stream tab_bulk2;

drop stream if exists tab_bulk3;
create stream tab_bulk3(col nullable(bool), INDEX col_idx col TYPE set(0))
    settings flush_threshold_count=1;
insert into tab_bulk3(col) values (DEFAULT), (DEFAULT), (false);
select sleep(1) format Null;
select count() from table(tab_bulk3) where col or col is null;
drop stream tab_bulk3;

SELECT 'Bulk filtering disabled';
set secondary_indices_enable_bulk_filtering = 0;

drop stream if exists tab_nobulk1;
create stream tab_nobulk1(col nullable(bool), INDEX col_idx col TYPE set(0))
    settings flush_threshold_count=1;
insert into tab_nobulk1(col) values (DEFAULT), (DEFAULT);
select sleep(1) format Null;
select count() from table(tab_nobulk1) where col or col is null;
drop stream tab_nobulk1;

drop stream if exists tab_nobulk2;
create stream tab_nobulk2(col nullable(bool), INDEX col_idx col TYPE set(0))
    settings flush_threshold_count=1;
insert into tab_nobulk2(col) values (DEFAULT), (DEFAULT), (true);
select sleep(1) format Null;
select count() from table(tab_nobulk2) where col or col is null;
drop stream tab_nobulk2;

drop stream if exists tab_nobulk3;
create stream tab_nobulk3(col nullable(bool), INDEX col_idx col TYPE set(0))
    settings flush_threshold_count=1;
insert into tab_nobulk3(col) values (DEFAULT), (DEFAULT), (false);
select sleep(1) format Null;
select count() from table(tab_nobulk3) where col or col is null;
drop stream tab_nobulk3;
