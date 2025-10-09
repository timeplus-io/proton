SET query_mode='table';
set asterisk_include_reserved_columns=false;

--{ echoOn }
DROP STREAM IF EXISTS fill;
CREATE STREAM fill (date date, val int, str string);
select sleep(2) FORMAT Null;
INSERT INTO fill(date, val, str) VALUES (to_date16('2019-05-24'), 13, 'sd0')(to_date16('2019-05-10'), 16, 'vp7')(to_date16('2019-05-25'), 17, '0ei')(to_date16('2019-05-30'), 18, '3kd')(to_date16('2019-05-15'), 27, 'enb')(to_date16('2019-06-04'), 5, '6az')(to_date16('2019-05-23'), 15, '01v')(to_date16('2019-05-08'), 28, 'otf')(to_date16('2019-05-19'), 20, 'yfh')(to_date16('2019-05-07'), 26, '2ke')(to_date16('2019-05-07'), 18, 'prh')(to_date16('2019-05-09'), 25, '798')(to_date16('2019-05-10'), 1, 'myj')(to_date16('2019-05-11'), 18, '3s2')(to_date16('2019-05-23'), 29, '72y');
select sleep(2) FORMAT Null;

-- *** STREAM without fill to compare ***
SELECT * FROM fill ORDER BY date, val;

-- Some useful cases

SELECT * FROM fill ORDER BY date WITH FILL, val;

SELECT * FROM fill ORDER BY date WITH FILL FROM to_date16('2019-05-01') TO to_date16('2019-05-31'), val WITH FILL;

SELECT * FROM fill ORDER BY date DESC WITH FILL, val WITH FILL FROM 1 TO 6;

-- Some weird cases

SELECT * FROM fill ORDER BY date DESC WITH FILL TO to_date16('2019-05-01') STEP -2, val DESC WITH FILL FROM 10 TO -5 STEP -3;

SELECT * FROM fill ORDER BY date WITH FILL TO to_date16('2019-06-23') STEP 3, val WITH FILL FROM -10 STEP 2;

DROP STREAM fill;
CREATE STREAM fill (a uint32, b int32);
select sleep(2) FORMAT Null;
INSERT INTO fill(a, b) VALUES (1, -2), (1, 3), (3, 2), (5, -1), (6, 5), (8, 0);
select sleep(2) FORMAT Null;

-- *** STREAM without fill to compare ***
SELECT * FROM fill ORDER BY a, b;

SELECT * FROM fill ORDER BY a WITH FILL, b WITH fill;

SELECT * FROM fill ORDER BY a WITH FILL, b WITH fill TO 6 STEP 2;

SELECT * FROM fill ORDER BY a WITH FILL STEP -1; -- { serverError 475 }
SELECT * FROM fill ORDER BY a WITH FILL FROM 10 TO 1; -- { serverError 475 }
SELECT * FROM fill ORDER BY a DESC WITH FILL FROM 1 TO 10; -- { serverError 475 }
SELECT * FROM fill ORDER BY a WITH FILL FROM -10 to 10; -- { serverError 475 }

DROP STREAM fill;
