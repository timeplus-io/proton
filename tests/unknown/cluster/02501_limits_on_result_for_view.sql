DROP STREAM IF EXISTS 02501_test;
DROP STREAM IF EXISTS 02501_dist;
DROP VIEW IF EXISTS 02501_view;


-- create local stream
CREATE STREAM 02501_test(`a` uint64) ENGINE = Memory;

-- create dist stream
CREATE STREAM 02501_dist(`a` uint64) ENGINE = Distributed(test_cluster_two_shards, current_database(), 02501_test);

-- create view
CREATE VIEW 02501_view(`a` uint64) AS SELECT a FROM 02501_dist;

-- insert data
insert into 02501_test values(5),(6),(7),(8);

-- test
SELECT * from 02501_view settings max_result_rows = 1; -- { serverError 396 }
SELECT sum(a) from 02501_view settings max_result_rows = 1;


DROP STREAM IF EXISTS 02501_test;
DROP STREAM IF EXISTS 02501_dist;
DROP VIEW IF EXISTS 02501_view;