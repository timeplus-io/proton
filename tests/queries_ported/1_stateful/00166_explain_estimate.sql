-- Tags: no-replicated-database
-- Tag no-replicated-database: Requires investigation

EXPLAIN ESTIMATE SELECT count() FROM table(test.hits) WHERE CounterID = 29103473;
EXPLAIN ESTIMATE SELECT count() FROM table(test.hits) WHERE CounterID != 29103473;
EXPLAIN ESTIMATE SELECT count() FROM table(test.hits) WHERE CounterID > 29103473;
EXPLAIN ESTIMATE SELECT count() FROM table(test.hits) WHERE CounterID < 29103473;
EXPLAIN ESTIMATE SELECT count() FROM table(test.hits) WHERE CounterID = 29103473 UNION ALL SELECT count() FROM table(test.hits) WHERE CounterID = 1704509;
