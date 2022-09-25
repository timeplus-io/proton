SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS insert;
create stream insert (i uint64, s string, u uuid, d date, t datetime, a array(uint32)) ;

INSERT INTO insert (i, s, u, d, t, a) VALUES (1, 'Hello', 'ab41bdd6-5cd4-11e7-907b-a6006ad3dba0', '2016-01-01', '2016-01-02 03:04:05', [1, 2, 3]), (1 + 1, concat('Hello', ', world'), to_uuid('00000000-0000-0000-0000-000000000000'), to_date('2016-01-01') + 1, to_start_of_minute(to_datetime('2016-01-02 03:04:05')), [[0,1],[2]][1]), (round(pi()), concat('hello', ', world!'), to_uuid(to_string('ab41bdd6-5cd4-11e7-907b-a6006ad3dba0')), to_date(to_datetime('2016-01-03 03:04:05')), to_start_of_hour(to_datetime('2016-01-02 03:04:05')), []), (4, 'World', 'ab41bdd6-5cd4-11e7-907b-a6006ad3dba0', '2016-01-04', '2016-12-11 10:09:08', [3,2,1]);
SELECT sleep(3);

SELECT * FROM insert ORDER BY i;
DROP STREAM insert;

-- Test the case where the VALUES are delimited by semicolon and a query follows
-- w/o newline. With most formats the query in the same line would be ignored or
-- lead to an error, but VALUES are an exception and support semicolon delimiter,
-- in addition to the newline.
create stream if not exists t_306 (a int) engine Memory;
insert into t_306 (a) values (1); select 11111;
select * from t_306;
drop stream if exists t_306;
