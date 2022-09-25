SELECT median_timing(t), median_timing_weighted(t, w) FROM (SELECT number AS t, number = 77 ? 10 : 1 AS w FROM system.numbers LIMIT 100);
SELECT quantile_timing(0.5)(t), quantile_timing_weighted(0.5)(t, w) FROM (SELECT number AS t, number = 77 ? 10 : 0 AS w FROM system.numbers LIMIT 100);
SELECT median_timing(t), median_timing_weighted(t, w) FROM (SELECT number AS t, number = 77 ? 0 : 0 AS w FROM system.numbers LIMIT 100);
SELECT quantiles_timing(0.5, 0.9)(t), quantiles_timing_weighted(0.5, 0.9)(t, w) FROM (SELECT number AS t, number = 77 ? 10 : 1 AS w FROM system.numbers LIMIT 100);
