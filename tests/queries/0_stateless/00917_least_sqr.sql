select array_reduce('simpleLinearRegression', [1, 2, 3, 4], [100, 110, 120, 130]);
select array_reduce('simpleLinearRegression', [1, 2, 3, 4], [100, 110, 120, 131]);
select array_reduce('simpleLinearRegression', [-1, -2, -3, -4], [-100, -110, -120, -130]);
select array_reduce('simpleLinearRegression', [5, 5.1], [6, 6.1]);
select array_reduce('simpleLinearRegression', [0], [0]);
select array_reduce('simpleLinearRegression', [3, 4], [3, 3]);
select array_reduce('simpleLinearRegression', [3, 3], [3, 4]);
select array_reduce('simpleLinearRegression', empty_array_uint8(), empty_array_uint8());
select array_reduce('simpleLinearRegression', [1, 2, 3, 4], [1000000000, 1100000000, 1200000000, 1300000000]);
