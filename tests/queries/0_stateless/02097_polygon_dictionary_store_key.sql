DROP STREAM IF EXISTS polygons_test_table;
create stream polygons_test_table
(
    key array(array(array(tuple(float64, float64)))),
    name string
) ;

INSERT INTO polygons_test_table VALUES ([[[(3, 1), (0, 1), (0, -1), (3, -1)]]], 'Value');

DROP DICTIONARY IF EXISTS polygons_test_dictionary_no_option;
CREATE DICTIONARY polygons_test_dictionary_no_option
(
    key array(array(array(tuple(float64, float64)))),
    name string
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE 'polygons_test_table'))
LAYOUT(POLYGON())
LIFETIME(0);

SELECT * FROM polygons_test_dictionary_no_option; -- {serverError 1}

DROP DICTIONARY IF EXISTS polygons_test_dictionary;
CREATE DICTIONARY polygons_test_dictionary
(
    key array(array(array(tuple(float64, float64)))),
    name string
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE 'polygons_test_table'))
LAYOUT(POLYGON(STORE_POLYGON_KEY_COLUMN 1))
LIFETIME(0);

SELECT * FROM polygons_test_dictionary;

DROP DICTIONARY polygons_test_dictionary_no_option;
DROP DICTIONARY polygons_test_dictionary;
DROP STREAM polygons_test_table;
