set param_rounding=1;

SELECT 1 AS x ORDER BY x WITH FILL STEP {rounding:uint32} SETTINGS allow_experimental_analyzer = 1;
SELECT 1 AS x ORDER BY x WITH FILL STEP {rounding:uint32} SETTINGS allow_experimental_analyzer = 0;
