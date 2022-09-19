SELECT DISTINCT toFloat64(array_join(['+inf', '+Inf', '+INF', '+infinity', '+Infinity']));
SELECT DISTINCT toFloat64(array_join(['-inf', '-Inf', '-INF', '-infinity', '-Infinity']));
SELECT DISTINCT toFloat64(array_join(['inf', 'Inf', 'INF', 'infinity', 'Infinity']));
