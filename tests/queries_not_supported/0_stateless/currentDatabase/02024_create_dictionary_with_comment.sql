-----------------------------------------------------------------------------------
-- Check that `DICTIONARY` can be created with a `COMMENT` clause
-- and comment is visible both in `comment` column of `system.dictionaries`
-- and `SHOW CREATE DICTIONARY`.
-----------------------------------------------------------------------------------

-- prerequisites
create stream source_table
(
    id uint64,
    value string
) ();

INSERT INTO source_table VALUES (1, 'First');
INSERT INTO source_table VALUES (2, 'Second');

DROP DICTIONARY IF EXISTS 2024_dictionary_with_comment;

CREATE DICTIONARY 2024_dictionary_with_comment
(
    id uint64,
    value string
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'source_table'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000)
COMMENT 'Test dictionary with comment';

SHOW CREATE DICTIONARY 2024_dictionary_with_comment;
SELECT comment FROM system.dictionaries WHERE name == '2024_dictionary_with_comment' AND database == currentDatabase();

DROP DICTIONARY IF EXISTS 2024_dictionary_with_comment;
