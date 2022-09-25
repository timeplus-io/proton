-- Tags: distributed

DROP STREAM IF EXISTS click_storage;
DROP STREAM IF EXISTS click_storage_dst;

create stream click_storage ( `PhraseID` uint64, `PhraseProcessedID` uint64 ALIAS if(PhraseID > 5, PhraseID, 0) ) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO click_storage SELECT number AS PhraseID from numbers(10);

create stream click_storage_dst ( `PhraseID` uint64, `PhraseProcessedID` uint64 ) ENGINE = Distributed(test_shard_localhost, currentDatabase(), 'click_storage');

SET prefer_localhost_replica = 1;
SELECT materialize(PhraseProcessedID)　FROM click_storage_dst;

SET prefer_localhost_replica = 0;
SELECT materialize(PhraseProcessedID)　FROM click_storage_dst;

DROP STREAM IF EXISTS click_storage;
DROP STREAM IF EXISTS click_storage_dst;
