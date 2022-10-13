SET optimize_move_to_prewhere = 1;
SELECT uniq(URL) from table(test.hits) WHERE TraficSourceID IN (7);
