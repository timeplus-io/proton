DROP STREAM IF EXISTS tracking_events_tmp;
DROP STREAM IF EXISTS open_events_tmp;

create stream tracking_events_tmp (`APIKey` uint32, `EventDate` date) ENGINE = MergeTree PARTITION BY toYYYYMM(EventDate) ORDER BY (APIKey, EventDate);
create stream open_events_tmp (`APIKey` uint32, `EventDate` date) ENGINE = MergeTree PARTITION BY toMonday(EventDate) ORDER BY (APIKey, EventDate);

insert into open_events_tmp select 2, '2020-07-10' from numbers(32);
insert into open_events_tmp select 2, '2020-07-11' from numbers(31);

insert into tracking_events_tmp select 2, '2020-07-10' from numbers(1881);
insert into tracking_events_tmp select 2, '2020-07-11' from numbers(1623);

SELECT EventDate
FROM
(
    SELECT EventDate
    FROM tracking_events_tmp AS t1
    WHERE (EventDate >= to_date('2020-07-10')) AND (EventDate <= to_date('2020-07-11')) AND (APIKey = 2)
    GROUP BY EventDate
)
FULL OUTER JOIN 
(
    SELECT EventDate
    FROM remote('127.0.0.{1,3}', currentDatabase(), open_events_tmp) AS t2
    WHERE (EventDate >= to_date('2020-07-10')) AND (EventDate <= to_date('2020-07-11')) AND (APIKey = 2)
    GROUP BY EventDate
        WITH TOTALS
) USING EventDate
ORDER BY EventDate
settings totals_mode = 'after_having_auto', group_by_overflow_mode = 'any', max_rows_to_group_by = 10000000, joined_subquery_requires_alias=0;


DROP STREAM IF EXISTS tracking_events_tmp;
DROP STREAM IF EXISTS open_events_tmp;
