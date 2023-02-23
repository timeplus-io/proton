drop stream if exists test;

create stream test (Printer low_cardinality(string), intervalStart DateTime) engine MergeTree partition by (hiveHash(Printer), toYear(intervalStart)) order by (Printer, intervalStart);

insert into test values ('printer1', '2006-02-07 06:28:15');

select Printer from test where Printer='printer1';

drop stream test;
