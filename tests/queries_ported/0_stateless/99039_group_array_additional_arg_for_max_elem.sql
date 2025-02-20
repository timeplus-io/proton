-- Using group_array with numbers
SELECT group_array(number)
FROM numbers(5);  -- This will give you [0,1,2,3,4]

-- With max_size limit
SELECT group_array(number, 3)
FROM numbers(5);  -- This will give you [0,1,2]


SELECT group_array([1,2], 1);

-- Instead, you could do:
SELECT group_array(number + 1, 1)
FROM numbers(2);  -- This will give you [1]

-- below test is not stable, so i will hide it.
-- SELECT group_array(x, 1) 
-- FROM (SELECT 1 AS x UNION ALL SELECT 2);


drop stream if exists test99039;
create stream test99039(id int);
select sleep(3) FORMAT Null;
insert into test99039(id) values (1)(2)(3)(4)(5);
select sleep(3) FORMAT Null;
select group_array(id, 3) from table(test99039);
select group_array(id) from table(test99039);
select group_array(id, 0) from table(test99039); -- { serverError BAD_ARGUMENTS }
select group_array(id, -1) from table(test99039); -- { serverError BAD_ARGUMENTS }




-- Using group_uniq_array with numbers
SELECT group_uniq_array(number)
FROM numbers(5);  -- This will give you [0,4,2,1,3]

-- With max_size limit
SELECT group_uniq_array(number, 3)
FROM numbers(5);  -- This will give you [0,2,1]


SELECT group_uniq_array([1,2], 1);

-- Instead, you could do:
SELECT group_uniq_array(number + 1, 1)
FROM numbers(2);  -- This will give you [1]

-- below test is not stable, so i will hide it.
-- SELECT group_uniq_array(x, 1) 
-- FROM (SELECT 1 AS x UNION ALL SELECT 2);


