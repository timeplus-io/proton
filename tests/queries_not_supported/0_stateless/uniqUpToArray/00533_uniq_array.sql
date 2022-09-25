SELECT uniq_array([0, 1, 1], [0, 1, 1], [0, 1, 1]);
SELECT uniq_array([0, 1, 1], [0, 1, 1], [0, 1, 0]);
SELECT uniq_exact_array([0, 1, 1], [0, 1, 1], [0, 1, 1]);
SELECT uniq_exact_array([0, 1, 1], [0, 1, 1], [0, 1, 0]);
SELECT uniqUpToArray(10)([0, 1, 1], [0, 1, 1], [0, 1, 1]);
SELECT uniqUpToArray(10)([0, 1, 1], [0, 1, 1], [0, 1, 0]);
