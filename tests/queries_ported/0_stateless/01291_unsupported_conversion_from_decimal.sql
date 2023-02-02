SELECT to_interval_second(now64()); -- { serverError 70 }
SELECT CAST(now64() AS interval_second); -- { serverError 70 }

SELECT to_interval_second(now64()); -- { serverError 70 }
SELECT CAST(now64() AS interval_second); -- { serverError 70 }
