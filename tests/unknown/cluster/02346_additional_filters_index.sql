-- Tags: distributed

create stream stream_1 (x uint32, y string, INDEX a (length(y)) TYPE minmax GRANULARITY 1) engine = MergeTree order by x settings index_granularity = 2;
insert into stream_1 values (1, 'a'), (2, 'bb'), (3, 'ccc'), (4, 'dddd');

CREATE STREAM distr_table (x uint32, y string) ENGINE = Distributed(test_cluster_two_shards, current_database(), 'table_1');

-- { echoOn }
set max_rows_to_read = 2;

select * from stream_1 order by x settings additional_table_filters={'table_1' : 'x > 3'};
select * from stream_1 order by x settings additional_table_filters={'table_1' : 'x < 3'};

select * from stream_1 order by x settings additional_table_filters={'table_1' : 'length(y) >= 3'};
select * from stream_1 order by x settings additional_table_filters={'table_1' : 'length(y) < 3'};

set max_rows_to_read = 4;

select * from distr_table order by x settings additional_table_filters={'distr_table' : 'x > 3'};
select * from distr_table order by x settings additional_table_filters={'distr_table' : 'x < 3'};

select * from distr_table order by x settings additional_table_filters={'distr_table' : 'length(y) > 3'};
select * from distr_table order by x settings additional_table_filters={'distr_table' : 'length(y) < 3'};

