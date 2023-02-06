WITH min_sample_size_continous(20, 10, 0.05, 0.8, 0.05) AS res SELECT 'continous const 1', round_bankers(res.1, 2), round_bankers(res.2, 2), round_bankers(res.3, 2);
WITH min_sample_size_continous(0.0, 10.0, 0.05, 0.8, 0.05) AS res SELECT 'continous const 2', round_bankers(res.1, 2), round_bankers(res.2, 2), round_bankers(res.3, 2);
WITH min_sample_size_continous(20, 10.0, 0.05, 0.8, 0.05) AS res SELECT 'continous const 3', round_bankers(res.1, 2), round_bankers(res.2, 2), round_bankers(res.3, 2);
WITH min_sample_size_continous(20.0, 10, 0.05, 0.8, 0.05) AS res SELECT 'continous const 4', round_bankers(res.1, 2), round_bankers(res.2, 2), round_bankers(res.3, 2);

DROP STREAM IF EXISTS minimum_sample_size_continuos;
CREATE STREAM minimum_sample_size_continuos (baseline uint64, sigma uint64) ENGINE = Memory();
INSERT INTO minimum_sample_size_continuos VALUES (20, 10);
INSERT INTO minimum_sample_size_continuos VALUES (200, 10);
WITH min_sample_size_continous(baseline, sigma, 0.05, 0.8, 0.05) AS res SELECT 'continous uint64 1', round_bankers(res.1, 2), round_bankers(res.2, 2), round_bankers(res.3, 2) FROM minimum_sample_size_continuos ORDER BY round_bankers(res.1, 2);
WITH min_sample_size_continous(20, sigma, 0.05, 0.8, 0.05) AS res SELECT 'continous uint64 2', round_bankers(res.1, 2), round_bankers(res.2, 2), round_bankers(res.3, 2) FROM minimum_sample_size_continuos ORDER BY round_bankers(res.1, 2);
WITH min_sample_size_continous(baseline, 10, 0.05, 0.8, 0.05) AS res SELECT 'continous uint64 3', round_bankers(res.1, 2), round_bankers(res.2, 2), round_bankers(res.3, 2) FROM minimum_sample_size_continuos ORDER BY round_bankers(res.1, 2);
DROP STREAM IF EXISTS minimum_sample_size_continuos;

DROP STREAM IF EXISTS minimum_sample_size_continuos;
CREATE STREAM minimum_sample_size_continuos (baseline float64, sigma float64) ENGINE = Memory();
INSERT INTO minimum_sample_size_continuos VALUES (20, 10);
INSERT INTO minimum_sample_size_continuos VALUES (200, 10);
WITH min_sample_size_continous(baseline, sigma, 0.05, 0.8, 0.05) AS res SELECT 'continous float64 1', round_bankers(res.1, 2), round_bankers(res.2, 2), round_bankers(res.3, 2) FROM minimum_sample_size_continuos ORDER BY round_bankers(res.1, 2);
WITH min_sample_size_continous(20, sigma, 0.05, 0.8, 0.05) AS res SELECT 'continous float64 2', round_bankers(res.1, 2), round_bankers(res.2, 2), round_bankers(res.3, 2) FROM minimum_sample_size_continuos ORDER BY round_bankers(res.1, 2);
WITH min_sample_size_continous(baseline, 10, 0.05, 0.8, 0.05) AS res SELECT 'continous uint64 3', round_bankers(res.1, 2), round_bankers(res.2, 2), round_bankers(res.3, 2) FROM minimum_sample_size_continuos ORDER BY round_bankers(res.1, 2);
DROP STREAM IF EXISTS minimum_sample_size_continuos;

WITH min_sample_size_conversion(0.9, 0.01, 0.8, 0.05) AS res SELECT 'conversion const 1', round_bankers(res.1, 2), round_bankers(res.2, 2), round_bankers(res.3, 2);
WITH min_sample_size_conversion(0.0, 0.01, 0.8, 0.05) AS res SELECT 'conversion const 2', round_bankers(res.1, 2), round_bankers(res.2, 2), round_bankers(res.3, 2);

DROP STREAM IF EXISTS minimum_sample_size_conversion;
CREATE STREAM minimum_sample_size_conversion (p1 float64) ENGINE = Memory();
INSERT INTO minimum_sample_size_conversion VALUES (0.9);
INSERT INTO minimum_sample_size_conversion VALUES (0.8);
WITH min_sample_size_conversion(p1, 0.01, 0.8, 0.05) AS res SELECT 'conversion float64 1', round_bankers(res.1, 2), round_bankers(res.2, 2), round_bankers(res.3, 2) FROM minimum_sample_size_conversion ORDER BY round_bankers(res.1, 2);
WITH min_sample_size_conversion(0.9, 0.01, 0.8, 0.05) AS res SELECT 'conversion float64 2', round_bankers(res.1, 2), round_bankers(res.2, 2), round_bankers(res.3, 2) FROM minimum_sample_size_conversion ORDER BY round_bankers(res.1, 2);
DROP STREAM IF EXISTS minimum_sample_size_conversion;
