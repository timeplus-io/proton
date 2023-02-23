DROP STREAM IF EXISTS test_graphite;
create stream test_graphite (key uint32, Path string, Time DateTime('UTC'), Value uint8, Version uint32, col uint64)
    engine = GraphiteMergeTree('graphite_rollup') order by key;

INSERT INTO test_graphite (key) VALUES (0); -- { serverError BAD_ARGUMENTS }
DROP STREAM test_graphite;
