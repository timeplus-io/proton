-- { echo }

-- Aggregate function 'avg' allows overflow with two's complement arithmetics.
-- This contradicts the standard SQL semantic and we are totally fine with it.

-- aggregate_functionAvg::add
SELECT avg(-8000000000000000000) FROM (SELECT *, 1 AS k FROM numbers(65535*2)) GROUP BY k;
-- aggregate_functionAvg::addBatchSinglePlace
SELECT avg(-8000000000000000000) FROM numbers(65535 * 2);
-- aggregate_functionAvg::addBatchSinglePlaceNotNull
SELECT avg(to_nullable(-8000000000000000000)) FROM numbers(65535 * 2);
