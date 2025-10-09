select histogram(number-10, 5) from (select * from system.numbers limit 20);
select histogram(number, 5) from (select * from system.numbers limit 20);

WITH array_join(histogram(sin(number), 3)) AS res select round(res.1, 2), round(res.2, 2), round(res.3, 2) from (select * from system.numbers limit 10);
WITH array_join(histogram(sin(number-40), 1)) AS res SELECT round(res.1, 2), round(res.2, 2), round(res.3, 2) from (select * from system.numbers limit 80);

SELECT histogram(-2, 10);
