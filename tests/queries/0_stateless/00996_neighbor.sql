SELECT number, neighbor(to_string(number), 0) FROM numbers(10);

SELECT number, neighbor(to_string(number), 5) FROM numbers(10);
SELECT number, neighbor(to_string(number), -5) FROM numbers(10);

SELECT number, neighbor(to_string(number), 10) FROM numbers(10);
SELECT number, neighbor(to_string(number), -10) FROM numbers(10);

SELECT number, neighbor(to_string(number), 15) FROM numbers(10);
SELECT number, neighbor(to_string(number), -15) FROM numbers(10);

SELECT number, neighbor(to_string(number), 5, 'Hello') FROM numbers(10);
SELECT number, neighbor(to_string(number), -5, 'World') FROM numbers(10);

SELECT number, neighbor(to_string(number), 5, concat('Hello ', to_string(number))) FROM numbers(10);
SELECT number, neighbor(to_string(number), -5, concat('World ', to_string(number))) FROM numbers(10);


SELECT number, neighbor('ClickHouse', 0) FROM numbers(10);

SELECT number, neighbor('ClickHouse', 5) FROM numbers(10);
SELECT number, neighbor('ClickHouse', -5) FROM numbers(10);

SELECT number, neighbor('ClickHouse', 10) FROM numbers(10);
SELECT number, neighbor('ClickHouse', -10) FROM numbers(10);

SELECT number, neighbor('ClickHouse', 15) FROM numbers(10);
SELECT number, neighbor('ClickHouse', -15) FROM numbers(10);

SELECT number, neighbor('ClickHouse', 5, 'Hello') FROM numbers(10);
SELECT number, neighbor('ClickHouse', -5, 'World') FROM numbers(10);

SELECT number, neighbor('ClickHouse', 5, concat('Hello ', to_string(number))) FROM numbers(10);
SELECT number, neighbor('ClickHouse', -5, concat('World ', to_string(number))) FROM numbers(10);


SELECT number, neighbor(to_string(number), number) FROM numbers(10);
SELECT number, neighbor(to_string(number), int_div(number, 2)) FROM numbers(10);

SELECT number, neighbor('Hello', number) FROM numbers(10);
SELECT number, neighbor('Hello', -3) FROM numbers(10);
SELECT number, neighbor('Hello', -3, 'World') FROM numbers(10);
