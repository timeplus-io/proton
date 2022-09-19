SET query_mode = 'table';
drop stream if exists data;
create stream data (ts DateTime, field string, num_field Nullable(float64)) ENGINE = MergeTree() PARTITION BY ts ORDER BY ts;
insert into data values(to_datetime('2020-05-14 02:08:00'),'some_field_value',7.);
SELECT field, count_if(num_field > 6.0) FROM data PREWHERE (num_field>6.0) GROUP BY field;
drop stream if exists data;
