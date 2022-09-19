-- Tags: no-parallel

DROP STREAM IF EXISTS reserved_word_table;
create stream reserved_word_table (`index` uint8) ENGINE = MergeTree ORDER BY `index`;

DETACH TABLE reserved_word_table;
ATTACH TABLE reserved_word_table;

DROP STREAM reserved_word_table;
