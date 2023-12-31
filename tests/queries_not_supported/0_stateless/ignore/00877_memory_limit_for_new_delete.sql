-- Tags: no-tsan, no-asan, no-msan, no-parallel, no-fasttest
-- Tag no-msan: memory limits don't work correctly under msan because it replaces malloc/free

SET max_memory_usage = 1000000000;

SELECT sum(ignore(*)) FROM (
    SELECT number, arg_max(number, (number, to_fixed_string(to_string(number), 1024)))
    FROM numbers(1000000)
    GROUP BY number
) -- { serverError 241 }
