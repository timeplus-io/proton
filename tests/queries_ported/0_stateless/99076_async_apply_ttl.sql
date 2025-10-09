drop stream if exists test_ttl;

CREATE STREAM test_ttl (
    id uint64,
    data string,
    created_at DateTime
);

-- Use FIXED timestamps for deterministic CI testing
INSERT INTO test_ttl(id, data, created_at)
SELECT 
    number as id,
    concat('data_', to_string(number)) as data,
    CASE 
        WHEN number % 2 = 0 THEN '2024-06-01 10:00:00'  -- Old, should expire
        ELSE '2024-06-08 10:00:00'                       -- Old, should expire
    END as created_at
FROM numbers_mt(10);

SELECT sleep(3) FORMAT Null;
SELECT sleep(3) FORMAT Null;


-- Debug: Show what should happen
SELECT 'Data that should expire (even IDs):' as info, count() as cnt 
FROM table(test_ttl) 
WHERE created_at + INTERVAL 1 DAY < now();

SELECT 'Data that should be kept (odd IDs):' as info, count() as cnt 
FROM table(test_ttl) 
WHERE created_at + INTERVAL 1 DAY >= now();

-- Apply TTL
ALTER STREAM test_ttl MODIFY TTL created_at + INTERVAL 1 DAY;
SELECT sleep(2) FORMAT Null;

-- Results before optimize
SELECT 'Before optimize result:' as stage, count() as remaining FROM table(test_ttl);
SELECT 'Before optimize remaining data:' as info, id, data, created_at FROM table(test_ttl) ORDER BY id;

OPTIMIZE stream test_ttl FINAL;
SELECT sleep(2) FORMAT Null;

-- Results after optimize
SELECT 'After optimize final result:' as stage, count() as remaining FROM table(test_ttl);
SELECT 'After optimize remaining data:' as info, id, data, created_at FROM table(test_ttl) ORDER BY id;

drop stream if exists test_ttl;


drop view   if exists 99076mv;
drop stream if exists 99076v;
drop stream if exists 99076target;
create stream 99076v(id int);
create stream 99076target(id int);
create materialized view 99076mv into 99076target as select * from 99076v;

ALTER VIEW 99076mv MODIFY TTL created_at + INTERVAL 1 DAY; -- { serverError NOT_IMPLEMENTED }

drop view   if exists 99076mv;
drop stream if exists 99076v;
drop stream if exists 99076target;
