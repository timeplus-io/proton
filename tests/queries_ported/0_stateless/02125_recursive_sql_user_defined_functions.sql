-- Tags: no-parallel

DROP FUNCTION IF EXISTS sql_02125_function;
CREATE FUNCTION sql_02125_function AS x -> sql_02125_function(x);
SELECT sql_02125_function(1); --{serverError 1};
DROP FUNCTION sql_02125_function;

DROP FUNCTION IF EXISTS sql_02125_function_1;
CREATE FUNCTION sql_02125_function_1 AS x -> sql_02125_function_2(x);

DROP FUNCTION IF EXISTS sql_02125_function_2;
CREATE FUNCTION sql_02125_function_2 AS x -> sql_02125_function_1(x);

SELECT sql_02125_function_1(1); --{serverError 1};
SELECT sql_02125_function_2(2); --{serverError 1};

CREATE OR REPLACE FUNCTION sql_02125_function_2 AS x -> x + 1;

SELECT sql_02125_function_1(1);
SELECT sql_02125_function_2(2);

DROP FUNCTION sql_02125_function_1;
DROP FUNCTION sql_02125_function_2;
