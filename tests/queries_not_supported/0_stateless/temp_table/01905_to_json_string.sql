-- Tags: no-fasttest

create temporary table t engine Memory as select * from generateRandom(
$$
    a array(int8),
    b uint32,
    c Nullable(string),
    d Decimal32(4),
    e Nullable(Enum16('h' = 1, 'w' = 5 , 'o' = -200)),
    f float64,
    g tuple(date, datetime('Europe/Moscow'), DateTime64(3, 'Europe/Moscow'), UUID),
    h FixedString(2),
    i array(Nullable(UUID))
$$, 10, 5, 3) limit 2;

select * apply toJSONString from t;

set allow_experimental_map_type = 1;
select toJSONString(map('1234', '5678'));
