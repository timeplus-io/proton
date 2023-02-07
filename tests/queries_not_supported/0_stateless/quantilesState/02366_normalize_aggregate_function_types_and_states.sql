SELECT count_merge(*) FROM (SELECT count_state(0.5) AS a UNION ALL SELECT count_state() UNION ALL SELECT count_if_state(2, 1) UNION ALL SELECT count_array_state([1, 2]) UNION ALL SELECT count_array_if_state([1, 2], 1));

SELECT quantileMerge(*) FROM (SELECT quantilesState(0.5)(1) AS a UNION ALL SELECT quantileStateIf(2, identity(1)));
