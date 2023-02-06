SELECT 1 / CAST(NULL, 'nullable(decimal(7, 2))');
SELECT materialize(1) / CAST(NULL, 'nullable(decimal(7, 2))');
SELECT 1 / CAST(materialize(NULL), 'nullable(decimal(7, 2))');
SELECT materialize(1) / CAST(materialize(NULL), 'nullable(decimal(7, 2))');


SELECT 1 / CAST(1, 'nullable(decimal(7, 2))');
SELECT materialize(1) / CAST(1, 'nullable(decimal(7, 2))');
SELECT 1 / CAST(materialize(1), 'nullable(decimal(7, 2))');
SELECT materialize(1) / CAST(materialize(1), 'nullable(decimal(7, 2))');


SELECT int_div(1, CAST(NULL, 'nullable(decimal(7, 2))'));
SELECT int_div(materialize(1), CAST(NULL, 'nullable(decimal(7, 2))'));
SELECT int_div(1, CAST(materialize(NULL), 'nullable(decimal(7, 2))'));
SELECT int_div(materialize(1), CAST(materialize(NULL), 'nullable(decimal(7, 2))'));


SELECT int_div(1, CAST(1, 'nullable(decimal(7, 2))'));
SELECT int_div(materialize(1), CAST(1, 'nullable(decimal(7, 2))'));
SELECT int_div(1, CAST(materialize(1), 'nullable(decimal(7, 2))'));
SELECT int_div(materialize(1), CAST(materialize(1), 'nullable(decimal(7, 2))'));


SELECT to_decimal32(1, 2) / CAST(NULL, 'nullable(uint32)');
SELECT materialize(to_decimal32(1, 2)) / CAST(NULL, 'nullable(uint32)');
SELECT to_decimal32(1, 2) / CAST(materialize(NULL), 'nullable(uint32)');
SELECT materialize(to_decimal32(1, 2)) / CAST(materialize(NULL), 'nullable(uint32)');


SELECT to_decimal32(1, 2) / CAST(1, 'nullable(uint32)');
SELECT materialize(to_decimal32(1, 2)) / CAST(1, 'nullable(uint32)');
SELECT to_decimal32(1, 2) / CAST(materialize(1), 'nullable(uint32)');
SELECT materialize(to_decimal32(1, 2)) / CAST(materialize(1), 'nullable(uint32)');


SELECT int_div(1, CAST(NULL, 'nullable(uint32)'));
SELECT int_div(materialize(1), CAST(NULL, 'nullable(uint32)'));
SELECT int_div(1, CAST(materialize(NULL), 'nullable(uint32)'));
SELECT int_div(materialize(1), CAST(materialize(NULL), 'nullable(uint32)'));


SELECT int_div(1, CAST(1, 'nullable(uint32)'));
SELECT int_div(materialize(1), CAST(1, 'nullable(uint32)'));
SELECT int_div(1, CAST(materialize(1), 'nullable(uint32)'));
SELECT int_div(materialize(1), CAST(materialize(1), 'nullable(uint32)'));


SELECT 1 % CAST(NULL, 'nullable(uint32)');
SELECT materialize(1) % CAST(NULL, 'nullable(uint32)');
SELECT 1 % CAST(materialize(NULL), 'nullable(uint32)');
SELECT materialize(1) % CAST(materialize(NULL), 'nullable(uint32)');


SELECT 1 % CAST(1, 'nullable(uint32)');
SELECT materialize(1) % CAST(1, 'nullable(uint32)');
SELECT 1 % CAST(materialize(1), 'nullable(uint32)');
SELECT materialize(1) % CAST(materialize(1), 'nullable(uint32)');


SELECT int_div(1, CAST(NULL, 'nullable(float32)'));
SELECT int_div(materialize(1), CAST(NULL, 'nullable(float32)'));
SELECT int_div(1, CAST(materialize(NULL), 'nullable(float32)'));
SELECT int_div(materialize(1), CAST(materialize(NULL), 'nullable(float32)'));


SELECT int_div(1, CAST(1, 'nullable(float32)'));
SELECT int_div(materialize(1), CAST(1, 'nullable(float32)'));
SELECT int_div(1, CAST(materialize(1), 'nullable(float32)'));
SELECT int_div(materialize(1), CAST(materialize(1), 'nullable(float32)'));


SELECT 1 % CAST(NULL, 'nullable(float32)');
SELECT materialize(1) % CAST(NULL, 'nullable(float32)');
SELECT 1 % CAST(materialize(NULL), 'nullable(float32)');
SELECT materialize(1) % CAST(materialize(NULL), 'nullable(float32)');


SELECT 1 % CAST(1, 'nullable(float32)');
SELECT materialize(1) % CAST(1, 'nullable(float32)');
SELECT 1 % CAST(materialize(1), 'nullable(float32)');
SELECT materialize(1) % CAST(materialize(1), 'nullable(float32)');


DROP STREAM IF EXISTS nullable_division;
CREATE STREAM nullable_division (x uint32, y nullable(uint32), a decimal(7, 2), b nullable(decimal(7, 2))) ENGINE=MergeTree() order by x;
INSERT INTO nullable_division VALUES (1, 1, 1, 1), (1, NULL, 1, NULL), (1, 0, 1, 0);

SELECT if(y = 0, 0, int_div(x, y)) from nullable_division;
SELECT if(y = 0, 0, x % y) from nullable_division;

SELECT if(y = 0, 0, int_div(a, y)) from nullable_division;
SELECT if(y = 0, 0, a / y) from nullable_division;

SELECT if(b = 0, 0, int_div(a, b)) from nullable_division;
SELECT if(b = 0, 0, a / b) from nullable_division;

SELECT if(b = 0, 0, int_div(x, b)) from nullable_division;
SELECT if(b = 0, 0, x / b) from nullable_division;

DROP STREAM nullable_division;
