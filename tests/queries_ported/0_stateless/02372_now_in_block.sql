SELECT count() FROM (SELECT DISTINCT now_in_block(), now_in_block('Pacific/Pitcairn') FROM system.numbers LIMIT 2);
SELECT now_in_block(1); -- { serverError 43 }
SELECT now_in_block(NULL) IS NULL;
SELECT now_in_block('UTC', 'UTC'); -- { serverError 42 }
