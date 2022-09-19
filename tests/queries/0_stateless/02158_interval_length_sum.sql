SELECT intervalLengthSum(x, y) FROM values('x int64, y int64', (0, 10), (5, 5), (5, 6), (1, -1));
