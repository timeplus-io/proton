SET max_memory_usage = '500M';
SELECT sum_map([number], [number]) FROM system.numbers_mt; -- { serverError 241 }
