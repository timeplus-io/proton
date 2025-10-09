drop stream if exists test;

create stream test (Printer low_cardinality(string), IntervalStart datetime) engine MergeTree partition by (hive_hash(Printer), to_year(IntervalStart)) order by (Printer, IntervalStart);

insert into test values ('printer1', '2006-02-07 06:28:15');

select Printer from test where Printer='printer1';

drop stream test;
