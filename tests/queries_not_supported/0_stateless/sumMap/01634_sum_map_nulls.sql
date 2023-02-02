SELECT initialize_aggregation('sumMap', [1, 2, 1], [1, 1, 1], [-1, null, 10]);
SELECT initialize_aggregation('sumMap', [1, 2, 1], [1, 1, 1], [-1, null, null]);
SELECT initialize_aggregation('sumMap', [1, 2, 1], [1, 1, 1], [null, null, null]); -- { serverError 43 }
SELECT initialize_aggregation('sumMap', [1, 2, 1], [1, 1, 1], [-1, 10, 10]);
SELECT initialize_aggregation('sumMap', [1, 2, 1], [1, 1, 1], [-1, 10, null]);
