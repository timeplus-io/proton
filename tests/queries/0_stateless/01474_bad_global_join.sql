-- Tags: global

DROP STREAM IF EXISTS local_table;
DROP STREAM IF EXISTS dist_table;

create stream local_table (id uint64, val string) ;

INSERT INTO local_table SELECT number AS id, to_string(number) AS val FROM numbers(100);

create stream dist_table AS local_table
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_table);

SELECT uniq(d.val) FROM dist_table AS d GLOBAL LEFT JOIN numbers(100) AS t USING id; -- { serverError 284 }
SELECT uniq(d.val) FROM dist_table AS d GLOBAL LEFT JOIN local_table AS t USING id;

DROP STREAM local_table;
DROP STREAM dist_table;
