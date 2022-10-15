SET max_bytes_to_read = 1000000000;

SET optimize_move_to_prewhere = 1;

SELECT uniq(URL) from table(test.hits) WHERE to_timezone(EventTime, 'Europe/Moscow') >= '2014-03-20 00:00:00' AND to_timezone(EventTime, 'Europe/Moscow') < '2014-03-21 00:00:00';
SELECT uniq(URL) from table(test.hits) WHERE to_timezone(EventTime, 'Europe/Moscow') >= '2014-03-20 00:00:00' AND URL != '' AND to_timezone(EventTime, 'Europe/Moscow') < '2014-03-21 00:00:00';
SELECT uniq(*) from table(test.hits) WHERE to_timezone(EventTime, 'Europe/Moscow') >= '2014-03-20 00:00:00' AND to_timezone(EventTime, 'Europe/Moscow') < '2014-03-21 00:00:00' AND EventDate = '2014-03-21';
WITH to_timezone(EventTime, 'Europe/Moscow') AS xyz SELECT uniq(*) from table(test.hits) WHERE xyz >= '2014-03-20 00:00:00' AND xyz < '2014-03-21 00:00:00' AND EventDate = '2014-03-21';

SET optimize_move_to_prewhere = 0;

SELECT uniq(URL) from table(test.hits) WHERE to_timezone(EventTime, 'Europe/Moscow') >= '2014-03-20 00:00:00' AND to_timezone(EventTime, 'Europe/Moscow') < '2014-03-21 00:00:00'; -- { serverError 307 }
SELECT uniq(URL) from table(test.hits) WHERE to_timezone(EventTime, 'Europe/Moscow') >= '2014-03-20 00:00:00' AND URL != '' AND to_timezone(EventTime, 'Europe/Moscow') < '2014-03-21 00:00:00'; -- { serverError 307 }
