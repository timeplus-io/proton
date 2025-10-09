DROP DICTIONARY IF EXISTS dict_flat_simple;
DROP DICTIONARY IF EXISTS dict_hashed_simple_decimal128;
DROP DICTIONARY IF EXISTS dict_hashed_simple_float32;
DROP DICTIONARY IF EXISTS dict_hashed_simple_string;
DROP DICTIONARY IF EXISTS dict_hashed_simple_auto_convert;
DROP STREAM IF EXISTS dict_data;

CREATE STREAM dict_data (v0 uint16, v1 int16, v2 float32, v3 decimal128(10), v4 string) engine=Memory()  AS SELECT number, number%65535, number*1.1, number*1.1, 'foo' FROM numbers(10);;

CREATE DICTIONARY dict_flat_simple (v0 uint16, v1 uint16, v2 uint16) PRIMARY KEY v0 SOURCE(TIMEPLUS(STREAM 'dict_data' USER 'proton' PASSWORD 'proton@t+')) LIFETIME(0) LAYOUT(flat());
SYSTEM RELOAD DICTIONARY dict_flat_simple;
SELECT name, type FROM system.dictionaries WHERE database = current_database() AND name = 'dict_flat_simple';
DROP DICTIONARY dict_flat_simple;

CREATE DICTIONARY dict_hashed_simple_decimal128 (v3 decimal128(10), v1 uint16, v2 float32) PRIMARY KEY v3 SOURCE(TIMEPLUS(STREAM 'dict_data' USER 'proton' PASSWORD 'proton@t+')) LIFETIME(0) LAYOUT(hashed());
SYSTEM RELOAD DICTIONARY dict_hashed_simple_decimal128;
SELECT name, type FROM system.dictionaries WHERE database = current_database() AND name = 'dict_hashed_simple_decimal128';
DROP DICTIONARY dict_hashed_simple_decimal128;

CREATE DICTIONARY dict_hashed_simple_float32 (v2 float32, v3 decimal128(10), v4 string) PRIMARY KEY v2 SOURCE(TIMEPLUS(STREAM 'dict_data' USER 'proton' PASSWORD 'proton@t+')) LIFETIME(0) LAYOUT(hashed());
SYSTEM RELOAD DICTIONARY dict_hashed_simple_float32;
SELECT name, type FROM system.dictionaries WHERE database = current_database() AND name = 'dict_hashed_simple_float32';
DROP DICTIONARY dict_hashed_simple_float32;

CREATE DICTIONARY dict_hashed_simple_string (v4 string, v3 decimal128(10), v2 float32) PRIMARY KEY v4 SOURCE(TIMEPLUS(STREAM 'dict_data' USER 'proton' PASSWORD 'proton@t+')) LIFETIME(0) LAYOUT(hashed());
SYSTEM RELOAD DICTIONARY dict_hashed_simple_string;
SELECT name, type FROM system.dictionaries WHERE database = current_database() AND name = 'dict_hashed_simple_string';
DROP DICTIONARY dict_hashed_simple_string;

CREATE DICTIONARY dict_hashed_simple_auto_convert (v0 uint16, v1 int16, v2 uint16) PRIMARY KEY v0,v1 SOURCE(TIMEPLUS(STREAM 'dict_data' USER 'proton' PASSWORD 'proton@t+')) LIFETIME(0) LAYOUT(hashed());
SYSTEM RELOAD DICTIONARY dict_hashed_simple_auto_convert;
SELECT name, type FROM system.dictionaries WHERE database = current_database() AND name = 'dict_hashed_simple_auto_convert';
DROP DICTIONARY dict_hashed_simple_auto_convert;

DROP STREAM dict_data;
