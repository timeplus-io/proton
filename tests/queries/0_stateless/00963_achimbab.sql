SET output_format_write_statistics = 0;

select 
    sum(cnt) > 0 as total,
    k[1], k[2]
    from
    (
        select
            array_map( x ->  x % 3 ? to_nullable(number%5 + x) : null, range(3)) as k,
            number % 4 ? to_nullable( rand() ) : Null  as cnt
        from system.numbers_mt
        where number < 1000000
        limit 1000000
    ) 
group by k with totals 
order by k[2]
SETTINGS max_threads = 100, max_execution_time = 120 
format JSON;
