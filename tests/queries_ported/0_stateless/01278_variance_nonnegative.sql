SELECT var_samp(0.1) FROM numbers(1000000);
SELECT var_pop(0.1) FROM numbers(1000000);
SELECT stddev_samp(0.1) FROM numbers(1000000);
SELECT stddev_pop(0.1) FROM numbers(1000000);

SELECT var_samp_stable(0.1) FROM numbers(1000000);
SELECT var_pop_stable(0.1) FROM numbers(1000000);
SELECT stddev_samp_stable(0.1) FROM numbers(1000000);
SELECT stddev_pop_stable(0.1) FROM numbers(1000000);
