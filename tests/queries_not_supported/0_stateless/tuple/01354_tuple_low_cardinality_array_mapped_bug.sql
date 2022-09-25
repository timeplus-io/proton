SELECT array_exists(x -> ((x.1) = 'pattern'), cast([tuple('a', 1)] as array(tuple(low_cardinality(string), uint8))));

DROP STREAM IF EXISTS table;
create stream table (id int32, values array(tuple(low_cardinality(string), int32)), date date) ENGINE MergeTree() PARTITION BY toYYYYMM(date) ORDER BY (id, date);

SELECT count(*) FROM table WHERE (array_exists(x -> ((x.1) = toLowCardinality('pattern')), values) = 1);

DROP STREAM IF EXISTS table;
