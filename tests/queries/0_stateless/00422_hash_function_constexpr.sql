SELECT cityHash64('abc') IN cityHash64('abc');
SELECT cityHash64(array_join(['abc', 'def'])) IN cityHash64('abc');
