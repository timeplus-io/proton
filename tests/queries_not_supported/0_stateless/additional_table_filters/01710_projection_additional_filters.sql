DROP STREAM IF EXISTS t;

CREATE STREAM t(a uint32, b uint32) ENGINE = MergeTree PARTITION BY a ORDER BY a;

INSERT INTO t SELECT number % 10, number FROM numbers(10000);

SELECT count(), min(a), max(a) FROM t SETTINGS additional_table_filters = {'t' : '0'};

DROP STREAM t;

drop stream if exists atf_p;
create stream atf_p (x uint64) engine = MergeTree order by tuple();
insert into atf_p select number from numbers(10);
select count() from atf_p settings additional_table_filters = {'atf_p': 'x <= 2'};
drop stream atf_p;
