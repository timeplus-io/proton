create stream test_extract(str string,  arr array(array(string)) ALIAS extractAllGroupsHorizontal(str, '\\W(\\w+)=("[^"]*?"|[^",}]*)')) ENGINE=MergeTree() PARTITION BY tuple() ORDER BY tuple();

INSERT INTO test_extract (str) WITH range(8) as range_arr, array_map(x-> concat(to_string(x),'Id'), range_arr) as key, array_map(x -> rand() % 8, range_arr) as val, array_string_concat(array_map((x,y) -> concat(x,'=',to_string(y)), key, val),',') as str SELECT str FROM numbers(500000);

ALTER STREAM test_extract ADD COLUMN `15Id` Nullable(uint16) DEFAULT toUInt16OrNull(array_first((v, k) -> (k = '4Id'), arr[2], arr[1]));

SELECT uniq(15Id) FROM test_extract SETTINGS max_threads=1, max_memory_usage=100000000;

SELECT uniq(15Id) FROM test_extract PREWHERE 15Id < 4 SETTINGS max_threads=1, max_memory_usage=100000000;

SELECT uniq(15Id) FROM test_extract WHERE 15Id < 4 SETTINGS max_threads=1, max_memory_usage=100000000;
