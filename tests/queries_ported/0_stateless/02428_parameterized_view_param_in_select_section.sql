-- https://github.com/ClickHouse/ClickHouse/issues/56564

create stream t(z string, ts datetime) Engine=Memory as 
select '1', '2020-01-01 00:00:00';

SELECT sleep(1) FORMAT Null;

CREATE VIEW v1 AS
SELECT z, 'test' = {m:string} AS c
FROM t 
WHERE ts > '2019-01-01 00:00:00'
GROUP BY z, c;

SELECT sleep(1) FORMAT Null;

CREATE VIEW v2 AS
SELECT z, {m:string} AS c
FROM t 
WHERE ts > '2019-01-01 00:00:00'
GROUP BY z, c;

SELECT sleep(1) FORMAT Null;

CREATE VIEW v3 AS
SELECT z, {m:string} 
FROM t;

SELECT sleep(1) FORMAT Null;

select * from v1(m='test');
select * from v2(m='test');
select * from v3(m='test');

drop stream t;
select sleep(1) FORMAT Null;
drop view v1;
drop view v2;
drop view v3;
select sleep(1) FORMAT Null;


create stream t(z string, ts datetime) as 
select '1', '2020-01-01 00:00:00', now();

SELECT sleep(1) FORMAT Null;

CREATE VIEW v1 AS
SELECT z, 'test' = {m:string} AS c
FROM t 
WHERE ts > '2019-01-01 00:00:00'
GROUP BY z, c;

SELECT sleep(1) FORMAT Null;

CREATE VIEW v2 AS
SELECT z, {m:string} AS c
FROM t 
WHERE ts > '2019-01-01 00:00:00'
GROUP BY z, c;

SELECT sleep(1) FORMAT Null;

CREATE VIEW v3 AS
SELECT z, {m:string} 
FROM t;

SELECT sleep(1) FORMAT Null;

SET query_mode='table';
select * from v1(m='test');
select * from v2(m='test');
select * from v3(m='test');

drop stream t;
select sleep(1) FORMAT Null;
drop view v1;
drop view v2;
drop view v3;
