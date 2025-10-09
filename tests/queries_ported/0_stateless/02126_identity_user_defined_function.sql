-- Tags: no-parallel

DROP FUNCTION IF EXISTS sql_02126_function;
CREATE FUNCTION sql_02126_function AS x -> x;
SELECT sql_02126_function(1);
DROP FUNCTION sql_02126_function;

CREATE FUNCTION sql_02126_function AS () -> x;
SELECT sql_02126_function(); --{ serverError 47 }
DROP FUNCTION sql_02126_function;

CREATE FUNCTION sql_02126_function AS () -> 5;
SELECT sql_02126_function();
DROP FUNCTION sql_02126_function;
