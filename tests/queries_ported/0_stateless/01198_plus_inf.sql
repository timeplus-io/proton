SELECT DISTINCT to_float64(array_join(['+inf', '+Inf', '+INF', '+infinity', '+Infinity']));
SELECT DISTINCT to_float64(array_join(['-inf', '-Inf', '-INF', '-infinity', '-Infinity']));
SELECT DISTINCT to_float64(array_join(['inf', 'Inf', 'INF', 'infinity', 'Infinity']));
