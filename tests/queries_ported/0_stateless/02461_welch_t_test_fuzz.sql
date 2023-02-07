
DROP STREAM IF EXISTS welch_ttest__fuzz_7;
CREATE STREAM welch_ttest__fuzz_7 (left uint128, right uint128) ENGINE = Memory;

INSERT INTO welch_ttest__fuzz_7 VALUES (0.010268, 0), (0.000167, 0), (0.000167, 0), (0.159258, 1), (0.136278, 1), (0.122389, 1);

SELECT round_bankers(welch_ttest(left, right).2, 6) from welch_ttest__fuzz_7;  -- { serverError 36 }
SELECT round_bankers(student_ttest(left, right).2, 6) from welch_ttest__fuzz_7;  -- { serverError 36 }
