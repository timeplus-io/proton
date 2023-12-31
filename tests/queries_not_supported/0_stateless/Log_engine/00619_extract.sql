
SELECT EXTRACT(DAY FROM to_date('2017-06-15'));
SELECT EXTRACT (MONTH FROM to_date('2017-06-15'));
SELECT EXTRACT(YEAR FROM to_date('2017-06-15'));
SELECT EXTRACT(SECOND FROM to_datetime('2017-12-31 18:59:58'));
SELECT EXTRACT(MINUTE FROM to_datetime('2017-12-31 18:59:58'));
SELECT EXTRACT(HOUR FROM to_datetime('2017-12-31 18:59:58'));
SELECT EXTRACT(DAY from to_datetime('2017-12-31 18:59:58'));
SELECT extract(MONTH FROM to_datetime('2017-12-31 18:59:58'));
SELECT EXTRACT(year FROM to_datetime('2017-12-31 18:59:58'));

SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS Orders;
create stream Orders (OrderId uint64, OrderName string, OrderDate datetime) engine = Log;
insert into Orders(OrderId, OrderName, OrderDate) values (1,   'Jarlsberg Cheese',    to_datetime('2008-10-11 13:23:44'));
select sleep(3);
SELECT EXTRACT(YYYY FROM OrderDate) AS OrderYear, EXTRACT(MONTH FROM OrderDate) AS OrderMonth, EXTRACT(DAY FROM OrderDate) AS OrderDay,
     EXTRACT(HOUR FROM OrderDate), EXTRACT(MINUTE FROM OrderDate), EXTRACT(SECOND FROM OrderDate) FROM Orders WHERE OrderId=1;
DROP STREAM Orders;


-- TODO:
-- SELECT EXTRACT(WEEK FROM to_date('2017-06-15'));
-- SELECT EXTRACT(WEEK FROM to_datetime('2017-12-31 18:59:58'));
-- SELECT EXTRACT(MINUTE_SECOND FROM to_datetime('2017-12-31 18:59:58'));
-- SELECT EXTRACT(HOUR_SECOND FROM to_datetime('2017-12-31 18:59:58'));
-- SELECT EXTRACT(HOUR_MINUTE FROM to_datetime('2017-12-31 18:59:58'));
-- SELECT EXTRACT(DAY_SECOND FROM to_datetime('2017-12-31 18:59:58'));
-- SELECT EXTRACT(DAY_MINUTE FROM to_datetime('2017-12-31 18:59:58'));
-- SELECT EXTRACT(DAY_HOUR FROM to_datetime('2017-12-31 18:59:58'));
-- SELECT EXTRACT(YEAR_MONTH FROM to_datetime('2017-12-31 18:59:58'));
-- SELECT EXTRACT(QUARTER FROM to_date('2017-06-15'));
-- SELECT EXTRACT(DAY_SECOND FROM to_date('2017-06-15'));
-- SELECT EXTRACT(DAY_MINUTE FROM to_date('2017-06-15'));
-- SELECT EXTRACT(DAY_HOUR FROM to_date('2017-06-15'));
-- SELECT EXTRACT(YEAR_MONTH FROM to_date('2017-06-15'));
-- SELECT EXTRACT(QUARTER FROM to_datetime('2017-12-31 18:59:58'));

-- Maybe:
-- SELECT EXTRACT (YEAR FROM DATE '2014-08-22') AS RESULT;
